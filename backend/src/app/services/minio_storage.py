"""MinIO Document Vault Service.

Provides asynchronous/thread-safe in-memory PDF generation via ReportLab,
object storage in MinIO, and presigned download URL generation.
"""

from datetime import datetime, timedelta, timezone
import io
import logging
from typing import Any
from minio import Minio
from minio.deleteobjects import DeleteObject
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from app.core.config import settings

logger = logging.getLogger(__name__)


class MinioStorageService:
    """Manages document storage in MinIO and in-memory boarding pass PDF rendering."""

    def __init__(self) -> None:
        self._client: Minio | None = None
        self._bucket_name: str = settings.MINIO_BUCKET_NAME

    @property
    def client(self) -> Minio:
        if self._client is None:
            self._client = Minio(
                endpoint=settings.minio_endpoint,
                access_key=settings.MINIO_ROOT_USER,
                secret_key=settings.MINIO_ROOT_PASSWORD,
                secure=settings.MINIO_SECURE,
            )
        return self._client

    def ensure_bucket_exists(self) -> None:
        """Verifies or creates the default empty document vault bucket."""
        try:
            if not self.client.bucket_exists(self._bucket_name):
                self.client.make_bucket(self._bucket_name)
                logger.info(f"Initialized MinIO bucket '{self._bucket_name}'.")
        except Exception as e:
            logger.warning(f"Could not verify/create MinIO bucket: {e}")

    def generate_boarding_pass_pdf(self, ticket_data: dict[str, Any]) -> bytes:
        """Renders an airline boarding pass PDF entirely in memory (io.BytesIO).

        Never writes temporary files to disk.
        """
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter

        # Background Header Banner
        c.setFillColor(colors.HexColor("#1A365D"))  # Deep Navy
        c.rect(0, height - 100, width, 100, fill=1, stroke=0)

        # Header Text
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 24)
        c.drawString(40, height - 55, "AIRPLANE TICKET & BOARDING PASS")
        c.setFont("Helvetica", 11)
        c.drawString(40, height - 78, "High-Concurrency Cloud Document Vault")

        # Accent Bar
        c.setFillColor(colors.HexColor("#2B6CB0"))
        c.rect(0, height - 105, width, 5, fill=1, stroke=0)

        # Passenger & Flight Details Box
        c.setStrokeColor(colors.HexColor("#CBD5E0"))
        c.setFillColor(colors.HexColor("#F7FAFC"))
        c.roundRect(35, height - 360, width - 70, 240, 8, fill=1, stroke=1)

        # Data Fields
        c.setFillColor(colors.HexColor("#2D3748"))
        c.setFont("Helvetica-Bold", 12)

        booking_code = ticket_data.get("booking_code", "N/A")
        ticket_id = ticket_data.get("ticket_id", "N/A")
        passenger_name = ticket_data.get("passenger_name", "N/A")
        flight_number = ticket_data.get("flight_number", "N/A")
        origin = ticket_data.get("departure_airport", "N/A")
        destination = ticket_data.get("arrival_airport", "N/A")
        departure_time = ticket_data.get("departure_time", "N/A")
        seat = ticket_data.get("seat_number", ticket_data.get("flight_seat_id", "N/A"))
        price = ticket_data.get("price", "0.00")
        status = ticket_data.get("status", "ACTIVE")

        # Row 1: Passenger & Booking Code
        c.drawString(55, height - 140, "PASSENGER NAME")
        c.setFont("Helvetica", 14)
        c.drawString(55, height - 160, str(passenger_name))

        c.setFont("Helvetica-Bold", 12)
        c.drawString(380, height - 140, "BOOKING REFERENCE (PNR)")
        c.setFont("Helvetica-Bold", 16)
        c.setFillColor(colors.HexColor("#2B6CB0"))
        c.drawString(380, height - 162, str(booking_code))

        # Divider
        c.setStrokeColor(colors.HexColor("#E2E8F0"))
        c.line(55, height - 180, width - 55, height - 180)

        # Row 2: Route & Flight
        c.setFillColor(colors.HexColor("#2D3748"))
        c.setFont("Helvetica-Bold", 11)
        c.drawString(55, height - 205, "FLIGHT")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(55, height - 225, str(flight_number))

        c.setFont("Helvetica-Bold", 11)
        c.drawString(180, height - 205, "ROUTE")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(180, height - 225, f"{origin}  ✈  {destination}")

        c.setFont("Helvetica-Bold", 11)
        c.drawString(380, height - 205, "SEAT")
        c.setFont("Helvetica-Bold", 18)
        c.setFillColor(colors.HexColor("#C53030"))
        c.drawString(380, height - 225, str(seat))

        # Row 3: Ticket ID & Departure
        c.setFillColor(colors.HexColor("#2D3748"))
        c.setFont("Helvetica-Bold", 11)
        c.drawString(55, height - 265, "TICKET NUMBER")
        c.setFont("Helvetica", 12)
        c.drawString(55, height - 285, f"TK-{ticket_id:08d}" if isinstance(ticket_id, int) else str(ticket_id))

        c.setFont("Helvetica-Bold", 11)
        c.drawString(180, height - 265, "DEPARTURE TIME")
        c.setFont("Helvetica", 12)
        c.drawString(180, height - 285, str(departure_time)[:19] if departure_time else "Scheduled")

        c.setFont("Helvetica-Bold", 11)
        c.drawString(380, height - 265, "PRICE & STATUS")
        c.setFont("Helvetica", 12)
        c.drawString(380, height - 285, f"${price} USD  [{status}]")

        # Barcode Simulation & Footer
        c.setStrokeColor(colors.HexColor("#A0AEC0"))
        c.setLineWidth(1)
        # Barcode lines
        import random
        rng = random.Random(hash(str(ticket_id)))
        x = 55
        for _ in range(70):
            bar_w = rng.choice([1, 2, 3])
            c.setLineWidth(bar_w)
            c.line(x, height - 340, x, height - 310)
            x += bar_w + rng.choice([1, 2])

        c.setFont("Helvetica-Oblique", 9)
        c.setFillColor(colors.HexColor("#718096"))
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        c.drawString(55, height - 355, f"Generated dynamically on purchase: {now_str}")

        c.save()
        buffer.seek(0)
        return buffer.getvalue()

    def upload_ticket_pdf(
        self,
        booking_code: str,
        ticket_id: int,
        pdf_bytes: bytes,
    ) -> str:
        """Uploads in-memory PDF bytes to MinIO. Returns the S3 object key."""
        object_key = f"tickets/{booking_code}/{ticket_id}.pdf"
        data_stream = io.BytesIO(pdf_bytes)
        self.ensure_bucket_exists()

        self.client.put_object(
            bucket_name=self._bucket_name,
            object_name=object_key,
            data=data_stream,
            length=len(pdf_bytes),
            content_type="application/pdf",
        )
        return object_key

    def get_presigned_download_url(
        self,
        object_key: str,
        expires_seconds: int = 900,
    ) -> str:
        """Generates a presigned GET download URL (default 15 minutes / 900s)."""
        url = self.client.get_presigned_url(
            method="GET",
            bucket_name=self._bucket_name,
            object_name=object_key,
            expires=timedelta(seconds=expires_seconds),
        )
        # If running inside Docker bridge network, adapt the host for external browser access
        if "minio:9000" in url:
            if settings.MINIO_PUBLIC_URL:
                url = url.replace("http://minio:9000", settings.MINIO_PUBLIC_URL)
            else:
                url = url.replace("http://minio:9000", f"http://localhost:{settings.MINIO_PORT}")
        return url

    def object_exists(self, object_key: str) -> bool:
        """Checks if an object exists in the document vault."""
        try:
            self.client.stat_object(self._bucket_name, object_key)
            return True
        except Exception:
            return False

    def get_object_bytes(self, object_key: str) -> bytes:
        """Retrieves object content bytes directly from MinIO."""
        response = self.client.get_object(self._bucket_name, object_key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def purge_all_objects(self) -> int:
        """Purges all stored objects inside the bucket while keeping the bucket intact."""
        try:
            if not self.client.bucket_exists(self._bucket_name):
                return 0
            objects = self.client.list_objects(self._bucket_name, recursive=True)
            delete_list = [DeleteObject(obj.object_name) for obj in objects]
            if not delete_list:
                return 0
            errors = list(self.client.remove_objects(self._bucket_name, delete_list))
            if errors:
                logger.warning(f"Errors occurred while purging MinIO objects: {errors}")
            return len(delete_list)
        except Exception as e:
            logger.warning(f"Error purging MinIO bucket: {e}")
            return 0


minio_service = MinioStorageService()
