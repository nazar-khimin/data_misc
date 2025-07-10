# Use the official Airflow image as a template.
FROM apache/airflow:3.0.0

# Switch to airflow user to install packages.
USER airflow

# Copy the requirements file into the container.
COPY ../requirements.txt .

# Install all dependencies from the requirements file.
RUN pip install -r ./requirements.txt