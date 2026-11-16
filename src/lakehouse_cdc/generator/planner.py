"""Deterministic OLTP workload plan for the loan-servicing schema (pure Python).

plan_workload() turns (seed, number of operations, starting ids) into a
sequence of transactions. It keeps its own model of the rows it has created,
so the plan needs no database access and the same inputs always produce the
same statements with the same parameters. The executor just runs them.

Not deterministic, by design: created_at / updated_at / changed_at use the
database clock (now()).
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

BASE_DATE = date(2024, 1, 1)
OPS_PER_SIM_DAY = 40

TERMINAL_STATUSES = frozenset({"PAID_OFF", "CHARGED_OFF"})
# Delinquency roll: (next status, weight)
TRANSITIONS: dict[str, tuple[tuple[str, int], ...]] = {
    "CURRENT": (("DPD_30", 1),),
    "DPD_30": (("CURRENT", 6), ("DPD_60", 4)),
    "DPD_60": (("CURRENT", 3), ("DPD_30", 2), ("DPD_90", 5)),
    "DPD_90": (("CURRENT", 1), ("DPD_60", 2), ("DEFAULT", 7)),
    "DEFAULT": (("CURRENT", 1), ("CHARGED_OFF", 9)),
}

PRODUCTS = (("FIXED_30", 360), ("FIXED_15", 180), ("ARM_5_1", 360), ("HELOC", 120))
STATES = ("CA", "TX", "FL", "NY", "WA", "IL", "GA", "NC", "AZ", "CO")
CHANNELS = ("ACH", "CARD", "CHECK", "WIRE")
FIRST_NAMES = ("Ana", "José", "Mei", "Zoë", "Liam", "Noor", "Olu", "Søren", "Priya", "D'Arcy")
LAST_NAMES = (
    "Okafor",
    "Nguyễn",
    "Müller",
    "Smith",
    "García",
    "O'Brien",
    "Kim",
    "Haddad",
    "李",
    "Rossi",
)

OP_WEIGHTS: tuple[tuple[str, int], ...] = (
    ("new_borrower", 5),
    ("update_borrower", 8),
    ("burst_update_borrower", 4),
    ("originate_loan", 10),
    ("post_payment", 40),
    ("reverse_payment", 4),
    ("roll_status", 15),
    ("delete_payment", 4),
    ("purge_loan", 2),
)

SCHEMA_CHANGE_SQL = "ALTER TABLE payments ADD COLUMN IF NOT EXISTS channel text"


@dataclass(frozen=True)
class Statement:
    sql: str
    params: tuple = ()


@dataclass(frozen=True)
class Txn:
    """Statements executed in one database transaction."""

    kind: str
    statements: tuple[Statement, ...]


@dataclass(frozen=True)
class PlanConfig:
    seed: int
    n_ops: int
    id_base: int = 0  # new ids are allocated above this value
    existing_borrowers: tuple[int, ...] = ()
    schema_change_at: int | None = None  # operation index; None disables the DDL
    channel_exists: bool = False  # True when payments.channel is already there


@dataclass
class _Loan:
    borrower_id: int
    status: str
    balance: Decimal
    rate: Decimal
    term_months: int
    next_due: date
    payments: list[int] = field(default_factory=list)


@dataclass
class _Payment:
    loan_id: int
    principal: Decimal
    status: str


class _Pool:
    """A set of ids with O(1) add, remove and seeded random choice."""

    def __init__(self, ids: tuple[int, ...] = ()) -> None:
        self._items: list[int] = list(ids)
        self._index: dict[int, int] = {v: i for i, v in enumerate(self._items)}

    def __contains__(self, item: int) -> bool:
        return item in self._index

    def add(self, item: int) -> None:
        if item not in self._index:
            self._index[item] = len(self._items)
            self._items.append(item)

    def discard(self, item: int) -> None:
        i = self._index.pop(item, None)
        if i is None:
            return
        last = self._items.pop()
        if i < len(self._items):
            self._items[i] = last
            self._index[last] = i

    def choice(self, rng: random.Random) -> int | None:
        return rng.choice(self._items) if self._items else None


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def plan_seed(n_borrowers: int, seed: int, id_base: int = 0) -> list[Txn]:
    """Borrowers inserted before the connector exists, so the initial snapshot has rows."""
    rng = random.Random(f"seed-{seed}")
    return [_new_borrower(rng, id_base + i + 1) for i in range(n_borrowers)]


def _new_borrower(rng: random.Random, borrower_id: int) -> Txn:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    return Txn(
        "new_borrower",
        (
            Statement(
                "INSERT INTO borrowers (borrower_id, full_name, email, state_code, credit_score,"
                " annual_income) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    borrower_id,
                    f"{first} {last}",
                    f"borrower{borrower_id}@example.com",
                    rng.choice(STATES),
                    rng.randint(520, 830),
                    _money(Decimal(rng.randint(35_000, 320_000))),
                ),
            ),
        ),
    )


class _Planner:
    def __init__(self, cfg: PlanConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(f"workload-{cfg.seed}")
        self.loans: dict[int, _Loan] = {}
        self.payments: dict[int, _Payment] = {}
        # Pools the operations pick their targets from.
        self.borrowers = _Pool(cfg.existing_borrowers)
        self.all_loans = _Pool()
        self.open_loans = _Pool()  # not in a terminal status; these always have a balance
        self.open_payments = _Pool()  # payments on open loans
        self.posted_payments = _Pool()  # ... that are still POSTED
        self.next_id = cfg.id_base
        self.has_channel = cfg.channel_exists
        self.today = BASE_DATE

    def _id(self) -> int:
        self.next_id += 1
        return self.next_id

    def _close(self, loan_id: int) -> None:
        """A loan reached a terminal status or was deleted: retire it and its payments."""
        self.open_loans.discard(loan_id)
        loan = self.loans.get(loan_id)
        for payment_id in loan.payments if loan else ():
            self.open_payments.discard(payment_id)
            self.posted_payments.discard(payment_id)

    def _history(
        self, loan_id: int, from_status: str | None, to_status: str, reason: str
    ) -> Statement:
        return Statement(
            "INSERT INTO loan_status_history (history_id, loan_id, from_status, to_status, reason)"
            " VALUES (%s, %s, %s, %s, %s)",
            (self._id(), loan_id, from_status, to_status, reason),
        )

    # -- operations ---------------------------------------------------------

    def new_borrower(self) -> Txn:
        borrower_id = self._id()
        self.borrowers.add(borrower_id)
        return _new_borrower(self.rng, borrower_id)

    def update_borrower(self) -> Txn:
        borrower_id = self.borrowers.choice(self.rng)
        if borrower_id is None:
            return self.new_borrower()
        return Txn(
            "update_borrower",
            (
                Statement(
                    "UPDATE borrowers SET credit_score = %s, is_active = %s, updated_at = now()"
                    " WHERE borrower_id = %s",
                    (self.rng.randint(480, 850), self.rng.random() > 0.05, borrower_id),
                ),
            ),
        )

    def burst_update_borrower(self) -> Txn:
        """Several updates to one row in one transaction: same commit, increasing LSNs."""
        borrower_id = self.borrowers.choice(self.rng)
        if borrower_id is None:
            return self.new_borrower()
        return Txn(
            "burst_update_borrower",
            tuple(
                Statement(
                    "UPDATE borrowers SET annual_income = %s, updated_at = now() WHERE borrower_id = %s",
                    (_money(Decimal(self.rng.randint(35_000, 320_000))), borrower_id),
                )
                for _ in range(3)
            ),
        )

    def originate_loan(self) -> Txn:
        borrower_id = self.borrowers.choice(self.rng)
        if borrower_id is None:
            return self.new_borrower()
        loan_id = self._id()
        product, term = self.rng.choice(PRODUCTS)
        principal = _money(Decimal(self.rng.randint(80, 900)) * 1000)
        rate = Decimal(self.rng.randint(3250, 8750)) / Decimal(100_000)
        next_due = self.today + timedelta(days=30)
        self.loans[loan_id] = _Loan(borrower_id, "CURRENT", principal, rate, term, next_due)
        self.all_loans.add(loan_id)
        self.open_loans.add(loan_id)
        return Txn(
            "originate_loan",
            (
                Statement(
                    "INSERT INTO loans (loan_id, borrower_id, product_type, property_state,"
                    " original_principal, principal_balance, interest_rate, term_months,"
                    " origination_date, next_due_date, status)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        loan_id,
                        borrower_id,
                        product,
                        self.rng.choice(STATES),
                        principal,
                        principal,
                        rate,
                        term,
                        self.today,
                        next_due,
                        "CURRENT",
                    ),
                ),
                self._history(loan_id, None, "CURRENT", "origination"),
            ),
        )

    def post_payment(self) -> Txn:
        loan_id = self.open_loans.choice(self.rng)
        if loan_id is None:
            return self.originate_loan()
        loan = self.loans[loan_id]
        payment_id = self._id()
        interest = _money(loan.balance * loan.rate / 12)
        # A slice of principal; one payment in 25 pays the loan off.
        if self.rng.random() < 0.04:
            principal = loan.balance
        else:
            principal = min(
                loan.balance, _money(loan.balance / loan.term_months * self.rng.randint(1, 4))
            )
        fee = _money(Decimal(self.rng.choice((0, 0, 0, 0, 25, 35))))
        amount = principal + interest + fee
        loan.balance -= principal
        loan.next_due = loan.next_due + timedelta(days=30)
        loan.payments.append(payment_id)
        self.payments[payment_id] = _Payment(loan_id, principal, "POSTED")
        self.open_payments.add(payment_id)
        self.posted_payments.add(payment_id)

        columns = ("payment_id, loan_id, payment_date, amount, principal_component,"
                   " interest_component, fee_component, status")  # fmt: skip
        params: tuple = (
            payment_id,
            loan_id,
            self.today,
            amount,
            principal,
            interest,
            fee,
            "POSTED",
        )
        if self.has_channel:
            columns += ", channel"
            params += (self.rng.choice(CHANNELS),)
        placeholders = ", ".join(["%s"] * len(params))
        statements = [
            Statement(f"INSERT INTO payments ({columns}) VALUES ({placeholders})", params),
            Statement(
                "UPDATE loans SET principal_balance = %s, next_due_date = %s, updated_at = now()"
                " WHERE loan_id = %s",
                (loan.balance, loan.next_due, loan_id),
            ),
        ]
        if loan.balance == 0:
            statements.append(
                Statement(
                    "UPDATE loans SET status = %s, updated_at = now() WHERE loan_id = %s",
                    ("PAID_OFF", loan_id),
                )
            )
            statements.append(self._history(loan_id, loan.status, "PAID_OFF", "payoff"))
            loan.status = "PAID_OFF"
            self._close(loan_id)
        return Txn("post_payment", tuple(statements))

    def reverse_payment(self) -> Txn:
        payment_id = self.posted_payments.choice(self.rng)
        if payment_id is None:
            return self.post_payment()
        payment = self.payments[payment_id]
        loan = self.loans[payment.loan_id]
        payment.status = "REVERSED"
        self.posted_payments.discard(payment_id)
        loan.balance += payment.principal
        return Txn(
            "reverse_payment",
            (
                Statement(
                    "UPDATE payments SET status = %s, updated_at = now() WHERE payment_id = %s",
                    ("REVERSED", payment_id),
                ),
                Statement(
                    "UPDATE loans SET principal_balance = %s, updated_at = now() WHERE loan_id = %s",
                    (loan.balance, payment.loan_id),
                ),
            ),
        )

    def roll_status(self) -> Txn:
        loan_id = self.open_loans.choice(self.rng)
        if loan_id is None:
            return self.originate_loan()
        loan = self.loans[loan_id]
        options = TRANSITIONS[loan.status]
        new_status = self.rng.choices([s for s, _ in options], weights=[w for _, w in options])[0]
        reason = "cure" if new_status == "CURRENT" else "delinquency_roll"
        txn = Txn(
            "roll_status",
            (
                Statement(
                    "UPDATE loans SET status = %s, updated_at = now() WHERE loan_id = %s",
                    (new_status, loan_id),
                ),
                self._history(loan_id, loan.status, new_status, reason),
            ),
        )
        loan.status = new_status
        if new_status in TERMINAL_STATUSES:
            self._close(loan_id)
        return txn

    def delete_payment(self) -> Txn:
        """Hard delete of a payment keyed in error. Restores the balance if it was applied."""
        payment_id = self.open_payments.choice(self.rng)
        if payment_id is None:
            return self.post_payment()
        payment = self.payments.pop(payment_id)
        self.open_payments.discard(payment_id)
        self.posted_payments.discard(payment_id)
        loan = self.loans[payment.loan_id]
        loan.payments.remove(payment_id)
        statements = [Statement("DELETE FROM payments WHERE payment_id = %s", (payment_id,))]
        if payment.status == "POSTED":
            loan.balance += payment.principal
            statements.append(
                Statement(
                    "UPDATE loans SET principal_balance = %s, updated_at = now() WHERE loan_id = %s",
                    (loan.balance, payment.loan_id),
                )
            )
        return Txn("delete_payment", tuple(statements))

    def purge_loan(self) -> Txn:
        """Hard delete of a loan and everything hanging off it (a rescinded loan)."""
        loan_id = self.all_loans.choice(self.rng)
        if loan_id is None:
            return self.originate_loan()
        self._close(loan_id)
        self.all_loans.discard(loan_id)
        loan = self.loans.pop(loan_id)
        for payment_id in loan.payments:
            self.payments.pop(payment_id, None)
        return Txn(
            "purge_loan",
            (
                Statement("DELETE FROM payments WHERE loan_id = %s", (loan_id,)),
                Statement("DELETE FROM loan_status_history WHERE loan_id = %s", (loan_id,)),
                Statement("DELETE FROM loans WHERE loan_id = %s", (loan_id,)),
            ),
        )

    def plan(self) -> Iterator[Txn]:
        names = [n for n, _ in OP_WEIGHTS]
        weights = [w for _, w in OP_WEIGHTS]
        for i in range(self.cfg.n_ops):
            self.today = BASE_DATE + timedelta(days=i // OPS_PER_SIM_DAY)
            if self.cfg.schema_change_at is not None and i == self.cfg.schema_change_at:
                self.has_channel = True
                yield Txn("schema_change", (Statement(SCHEMA_CHANGE_SQL),))
                continue
            yield getattr(self, self.rng.choices(names, weights=weights)[0])()


def plan_workload(cfg: PlanConfig) -> Iterator[Txn]:
    return _Planner(cfg).plan()
