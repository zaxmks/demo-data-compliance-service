def log(logger, message, use_disclaimer=False, use_delimiter=False):
    delimiter = "*****************************************************************************"
    if use_delimiter:
        message = delimiter + '\n' + message
    if use_disclaimer:
        future_disclaimer = "In the future, we will: "
        future_disclaimer += "\n-Remove the ingested PDF data"
        future_disclaimer += "\n- Log removals to the Main Ingestion DB"
        message += '\n' + future_disclaimer
    if use_delimiter:
        message += '\n' + delimiter
    logger.info(message)
