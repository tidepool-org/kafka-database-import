CREATE TABLE user_filter (
    created_time         TIMESTAMPTZ DEFAULT now(),

    user_id              TEXT NOT NULL ,
    partition            INT default 0
);


