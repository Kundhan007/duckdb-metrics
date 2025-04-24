# DuckDB Function Call Metrics

A simple implementation to track and analyze function execution metrics using DuckDB.

## Overview

This project provides a framework to monitor and analyze function execution metrics in Python using DuckDB as the storage backend.

### Features

- Track function execution time and status
- Record function errors and failures
- Store metrics in DuckDB for efficient querying
- Easy integration through decorators

## Components

- Python sample functions demonstrating the implementation
- DuckDB integration with decorator support
- Test suite for verification

## Sample Output

Below is an example of the metrics output showing the last 10 function calls:

this is sample output of the metrics:
| S.No | Function          | Start Time             | Duration (ms) | Status     | Error         |
|:----:|-------------------|------------------------|---------------|------------|---------------|
| 4    | failing_function  | April 24 21:14:30.338  | 0             | ❌ error    | Sample error  |
| 3    | sample_function   | April 24 21:14:28.819  | 1003          | ✅ success  | -             |
| 2    | sample_function   | April 24 21:14:27.297  | 1004          | ✅ success  | -             |
| 1    | sample_function   | April 24 21:14:25.722  | 1005          | ✅ success  | -             |
