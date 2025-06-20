resource "aws_mwaa_environment" "mwaa_env" {
  name                 = var.mwaa_name
  airflow_version      = "2.9.2"
  execution_role_arn   = aws_iam_role.mwaa_execution_role.arn
  source_bucket_arn    = aws_s3_bucket.data-bucket.arn
  max_workers          = 5
  min_workers          = 1
  environment_class    = "mw1.small"
  dag_s3_path          = "dags/"
  requirements_s3_path = "airflow/requirements.txt"
  startup_script_s3_path    = "startup.sh"
  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = [aws_subnet.private_subnet_1.id, aws_subnet.private_subnet_2.id]
  }
  logging_configuration {
    task_logs {
      enabled = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled = true
      log_level = "INFO"
    }
    worker_logs {
      enabled = true
      log_level = "INFO"
    }
    dag_processing_logs {
      enabled = true
      log_level = "INFO"
    }
  }
  webserver_access_mode = "PUBLIC_ONLY"
  tags = {
    Name = var.tag
  }
}