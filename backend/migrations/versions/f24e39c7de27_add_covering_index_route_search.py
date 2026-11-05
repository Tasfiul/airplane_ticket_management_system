"""add_covering_index_route_search

Revision ID: f24e39c7de27
Revises: 161930981597
Create Date: 2026-11-04 23:04:28.840820

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f24e39c7de27'
down_revision: Union[str, Sequence[str], None] = '161930981597'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema with composite covering index for route search."""
    op.create_index(
        "idx_flight_route_covering",
        "flight",
        ["departure_airport", "arrival_airport", "departure_time"],
        postgresql_include=["flight_id", "flight_number", "plane_id", "status"],
        if_not_exists=True,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "idx_flight_route_covering",
        table_name="flight",
        if_exists=True,
    )
