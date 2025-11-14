import math
from typing import Tuple

# Constants
EARTH_RADIUS_KM = 6371.0


class GeospatialCalculator:
    """Utility class for geospatial calculations."""

    @staticmethod
    def calculate_destination_coordinates(
        lat: float, lon: float, direction: float, distance_km: float = 5.0
    ) -> Tuple[float, float]:
        """
        Calculate destination coordinates given start point, direction, and distance.

        Args:
            lat: Starting latitude in degrees
            lon: Starting longitude in degrees
            direction: Direction in degrees (0-360)
            distance_km: Distance in kilometers

        Returns:
            Tuple of (destination_latitude, destination_longitude)
        """
        # Convert to radians
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        direction_rad = math.radians(direction)

        # Angular distance
        angular_distance = distance_km / EARTH_RADIUS_KM

        # Calculate destination latitude
        dest_lat_rad = math.asin(
            math.sin(lat_rad) * math.cos(angular_distance)
            + math.cos(lat_rad) * math.sin(angular_distance) * math.cos(direction_rad)
        )

        # Calculate destination longitude
        dest_lon_rad = lon_rad + math.atan2(
            math.sin(direction_rad) * math.sin(angular_distance) * math.cos(lat_rad),
            math.cos(angular_distance) - math.sin(lat_rad) * math.sin(dest_lat_rad),
        )

        return math.degrees(dest_lat_rad), math.degrees(dest_lon_rad)

    @staticmethod
    def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula."""
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return EARTH_RADIUS_KM * c
