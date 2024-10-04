# document_service/Dockerfile

FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Command to run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]
