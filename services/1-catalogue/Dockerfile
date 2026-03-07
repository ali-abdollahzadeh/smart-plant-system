# Use a lightweight Python image as the base
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire catalogue service code into the container
COPY . .

# Expose port 8000 (the port we configured in CherryPy)
EXPOSE 8000

# The command that runs when the container starts
CMD ["python", "main.py"]