import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

def update_db(data):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        for row in data:
            service_id = int(row["ServiceId"])
            journey_date = row.get("JourneyDate")
            day_of_exception = row.get("DayofException")
            time_change = row["timeChange"]
            boarding_ids = row["boardingIds"]
            dropping_ids = row["droppingIds"]
            print(f"serviceId, journeyDate, dayOfException, time Change, bp's, dp's: {service_id}",journey_date,day_of_exception,time_change,boarding_ids,dropping_ids)
            trip_ids = []

            if journey_date and not day_of_exception:  # Handle case when JourneyDate is provided
                cursor.execute(
                    """
                    SELECT id FROM "Trips"
                    WHERE "serviceId" = %s AND "journeyDate" = %s
                    """,
                    (service_id, journey_date),
                )
                trip_id = cursor.fetchone()[0]
                if boarding_ids:
                    cursor.execute(
                        sql.SQL(
                            """
                            UPDATE "TripBoardingPoints"
                            SET 
                                "scheduledTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceBoardingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripBoardingPoints"."tripId" 
                                    AND sbp."boardingPointId" = "TripBoardingPoints"."boardingPointId"
                                ),
                                "currentTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceBoardingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripBoardingPoints"."tripId" 
                                    AND sbp."boardingPointId" = "TripBoardingPoints"."boardingPointId"
                                )
                            WHERE "boardingPointId" = ANY(%s) 
                            AND "tripId" = %s
                            """
                        ).format(interval=sql.Literal(time_change)),
                        (boarding_ids, trip_id),
                    )
                    # Update dropping points
                if dropping_ids:
                    cursor.execute(
                        sql.SQL(
                            """
                            UPDATE "TripDroppingPoints"
                            SET 
                                "scheduledTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceDroppingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripDroppingPoints"."tripId" 
                                    AND sbp."droppingPointId" = "TripDroppingPoints"."droppingPointId"
                                ),
                                "currentTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceDroppingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripDroppingPoints"."tripId" 
                                    AND sbp."droppingPointId" = "TripDroppingPoints"."droppingPointId"
                                )
                            WHERE "droppingPointId" = ANY(%s) 
                            AND "tripId" = %s
                            """
                        ).format(interval=sql.Literal(time_change)),
                        (dropping_ids, trip_id),
                    )
                conn.commit()
                print("Boarding and Dropping Points updated successfully.")

            elif day_of_exception and not journey_date:  # Handle case when DayofException is provided
                cursor.execute(
                    """
                    SELECT id FROM "Trips"
                    WHERE "journeyDate" >= current_date
                      AND EXTRACT(DOW FROM "journeyDate") = ANY(%s)
                      AND "serviceId" = %s
                    """,
                    (day_of_exception, service_id),
                )
                trip_ids = [trip[0] for trip in cursor.fetchall()]

                if not trip_ids:
                    print(f"No trips found for ServiceId {service_id} with provided criteria.")
                    continue

                # Update boarding points
                if boarding_ids:
                    cursor.execute(
                        sql.SQL(
                            """
                            UPDATE "TripBoardingPoints"
                            SET 
                                "scheduledTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceBoardingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripBoardingPoints"."tripId" 
                                    AND sbp."boardingPointId" = "TripBoardingPoints"."boardingPointId"
                                ),
                                "currentTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceBoardingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripBoardingPoints"."tripId" 
                                    AND sbp."boardingPointId" = "TripBoardingPoints"."boardingPointId"
                                )
                            WHERE "boardingPointId" = ANY(%s) 
                            AND "tripId" = ANY(%s)
                            """
                        ).format(interval=sql.Literal(time_change)),
                        (boarding_ids, trip_ids),
                    )

                # Update dropping points
                if dropping_ids:
                    cursor.execute(
                        sql.SQL(
                            """
                            UPDATE "TripDroppingPoints"
                            SET 
                                "scheduledTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceDroppingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripDroppingPoints"."tripId" 
                                    AND sbp."droppingPointId" = "TripDroppingPoints"."droppingPointId"
                                ),
                                "currentTime" = (
                                    SELECT CASE 
                                        WHEN sbp."day" = 0 THEN t."journeyDate" + sbp."scheduledTime" - interval '330 minute'
                                        WHEN sbp."day" = 1 THEN t."journeyDate" + sbp."scheduledTime" + interval '1 day' - interval '330 minute'
                                    END + interval '{interval} minutes'
                                    FROM "Trips" t 
                                    JOIN "ServiceDroppingPoints" sbp 
                                    ON t."serviceId" = sbp."serviceId" 
                                    WHERE t.id = "TripDroppingPoints"."tripId" 
                                    AND sbp."droppingPointId" = "TripDroppingPoints"."droppingPointId"
                                )
                            WHERE "droppingPointId" = ANY(%s) 
                            AND "tripId" = ANY(%s)
                            """
                        ).format(interval=sql.Literal(time_change)),
                        (dropping_ids, trip_ids),
                    )
                conn.commit()
                print("Boarding and Dropping Points updated successfully.")
    except Exception as e:
        print(f"Error updating database: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()