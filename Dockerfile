# Use an official Python runtime as a parent image
FROM python:3.5-slim

# Set the working directory to /app
WORKDIR /generic_monitor_logs

# Copy the current directory contents into the container at /app
ADD . /generic_monitor_logs

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Make port 80 available to the world outside this container
EXPOSE 10405

# Define environment variable
# ENV NAME World

# Run app.py when the container launches
CMD ["python3", "startup.py"]