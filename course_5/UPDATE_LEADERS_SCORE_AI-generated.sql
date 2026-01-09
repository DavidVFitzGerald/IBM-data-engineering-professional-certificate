--#SET TERMINATOR @

CREATE OR REPLACE PROCEDURE UPDATE_LEADERS_SCORE (
    IN in_School_ID INTEGER,
    IN in_Leader_Score INTEGER
)
LANGUAGE SQL
BEGIN
    DECLARE v_Icon VARCHAR(11);

    -- Exit handler for rollback on error
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
    END;

    -- Validate input range
    IF in_Leader_Score < 0 OR in_Leader_Score > 99 THEN
        SIGNAL SQLSTATE '75000'
            SET MESSAGE_TEXT = 'Leader score must be between 0 and 99';
    END IF;

    -- Assign icon value using CASE
    SET v_Icon = CASE
                    WHEN in_Leader_Score BETWEEN 80 AND 99 THEN 'Very strong'
                    WHEN in_Leader_Score BETWEEN 60 AND 79 THEN 'Strong'
                    WHEN in_Leader_Score BETWEEN 40 AND 59 THEN 'Average'
                    WHEN in_Leader_Score BETWEEN 20 AND 39 THEN 'Weak'
                    ELSE 'Very weak'
                 END;

    -- Single update statement
    UPDATE CHICAGO_PUBLIC_SCHOOLS
    SET Leaders_Score = in_Leader_Score,
        Leaders_Icon  = v_Icon
    WHERE School_ID = in_School_ID;

    COMMIT;
END
@