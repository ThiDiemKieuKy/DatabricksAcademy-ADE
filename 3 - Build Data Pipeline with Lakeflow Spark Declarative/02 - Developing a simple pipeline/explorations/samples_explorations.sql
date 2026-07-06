-- Databricks notebook source
SELECT *,
        current_timestamp() AS processing_time,
        _metadata.file_name as source_file
FROM read_files(
    '/Volumes/workspace/dbacademy/retail_pipeline/orders',
    format => 'json'
)

