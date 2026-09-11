CREATE PROCEDURE metadata_logging
@run_id varchar(50),
@source_name varchar(100),
@source_layer varchar(50),
@target_layer varchar(50),
@source_path varchar(200),
@target_path varchar(200),
@start_time datetime,
@end_time datetime,
@status varchar(20),
@records_processed int,
@error_message varchar(max)
AS
BEGIN
    INSERT INTO dbo.logging_table
    (
        run_id,
        source_name,
        source_layer,
        target_layer,
        source_path,
        target_path,
        start_time,
        end_time,
        status,
        records_processed,
        error_message
    )
    VALUES
    (
        @run_id,
        @source_name,
        @source_layer,
        @target_layer,
        @source_path,
        @target_path,
        @start_time,
        @end_time,
        @status,
        @records_processed,
        @error_message
    );
END





