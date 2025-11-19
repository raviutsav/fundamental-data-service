FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency file and install everything
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your FastAPI project
COPY . .

# Expose the port your FastAPI app will run on
EXPOSE 8000

# Start the FastAPI server
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
