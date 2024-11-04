# Start with an official Python image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock (if present)
COPY pyproject.toml poetry.lock* ./

# Install system dependencies for psycopg2 and other packages
RUN apt-get update && apt-get install -y libpq-dev gcc

# Install Poetry
RUN pip install --no-cache-dir poetry

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Copy the entire project code to the container
COPY . .

# Ensure the .env file is included
COPY .env .env

# Set PYTHONPATH to the /app directory
ENV PYTHONPATH="/app"

# Expose the necessary port (update this if needed)
EXPOSE 8000

# Command to run your Python application (adjust as needed)
CMD ["poetry", "run", "python", "main.py"]