CREATE TABLE IF NOT EXISTS activities (
    id              BIGINT PRIMARY KEY,
    employee_id     BIGINT NOT NULL,
    start_date      TIMESTAMPTZ NOT NULL,
    activity_type   TEXT NOT NULL,
    distance_m      INTEGER NULL,
    end_date        TIMESTAMPTZ NOT NULL,
    comment         TEXT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activities_employee_id ON activities(employee_id);
CREATE INDEX IF NOT EXISTS idx_activities_start_date  ON activities(start_date);
CREATE INDEX IF NOT EXISTS idx_activities_type        ON activities(activity_type);
