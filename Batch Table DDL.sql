CREATE TABLE batch_table (
    BatchID INT PRIMARY KEY,
    BatchStartTimestamp TIMESTAMP,
    BatchEndTimestamp TIMESTAMP,
    BatchTransDatetime TIMESTAMP,
    BatchStatus VARCHAR(50)
);