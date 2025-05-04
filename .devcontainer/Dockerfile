FROM public.ecr.aws/emr-on-eks/spark/emr-7.5.0-spark-rapids:latest

# If the steps of a `Dockerfile` use files that are different from the `context` file, COPY the
# file of each step separately; and RUN the file immediately after COPY
WORKDIR /app
COPY .devcontainer/requirements.txt /app

# Setting-up
RUN pip install --upgrade pip && \
    pip install --requirement /app/requirements.txt --no-cache-dir

# Port
EXPOSE 8050

# ENTRYPOINT
ENTRYPOINT ["bash"]
