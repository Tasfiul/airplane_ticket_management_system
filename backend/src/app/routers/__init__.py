from app.routers.admin import router as admin_router
from app.routers.auth import router as auth_router
from app.routers.bookings import router as bookings_router
from app.routers.customers import router as customers_router
from app.routers.flights import router as flights_router
from app.routers.tickets import router as tickets_router

__all__ = [
    "admin_router",
    "auth_router",
    "customers_router",
    "flights_router",
    "bookings_router",
    "tickets_router",
]
