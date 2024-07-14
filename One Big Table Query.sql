SELECT COUNT(*) FROM (
    SELECT 
        A.booking_id
        , B.flightno
        , D.icao AS origin_icao
        , D.name AS origin_name
        , F.city AS origin_city
        , F.country AS origin_country
        , F.latitude AS origin_lat
        , F.longitude AS origin_long
        , E.icao AS destination_icao
        , E.name AS destination_name
        , G.city AS destination_city
        , G.country AS destination_country
        , G.latitude AS destination_lat
        , G.longitude AS destination_long
        , B.departure
        , B.arrival
        , H.airlinename
        , J.identifier AS airplane_type
        , I.capacity AS airplane_capacity
        , C.firstname
        , C.lastname
        , A.seat
        , C.passportno
        , K.birthdate as passenger_birthdate
        , K.sex as passenger_sex
        , K.country as passenger_country
        , K.city as passenger_city
        , A.price
    FROM booking A
    LEFT JOIN flight B on A.flight_id = B.flight_id
    LEFT JOIN passenger C on A.passenger_id = C.passenger_id
    LEFT JOIN passengerdetails K on A.passenger_id = K.passenger_id
    LEFT JOIN airport D on B.`from` = D.airport_id
    LEFT JOIN airport E on B.`to` = E.airport_id
    LEFT JOIN airport_geo F on B.`from` = F.airport_id
    LEFT JOIN airport_geo G on B.`to` = G.airport_id
    LEFT JOIN airline H on B.airline_id = H.airline_id
    LEFT JOIN airplane I on B.airplane_id = I.airplane_id
    LEFT JOIN airplane_type J on I.type_id = J.type_id
-- LIMIT 10
) ONE_BIG_TABLE;