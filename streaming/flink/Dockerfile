FROM pyflink-workshop:latest

USER root

# Fix ownership so we can install
RUN chown -R root:root /opt/pyflink/.venv

# Install Google Cloud libs into the exact venv Flink uses
RUN /opt/flink/.local/share/uv/python/cpython-3.12.13-linux-x86_64-gnu/bin/pip install \
    google-cloud-storage \
    google-cloud-bigquery \
    google-auth \
    db-dtypes \
    --upgrade \
    --target /opt/pyflink/.venv/lib/python3.12/site-packages/

# Fix google namespace package conflict
RUN cp -r /opt/pyflink/.venv/lib/python3.12/site-packages/google/cloud \
    /opt/pyflink/.venv/lib/python3.12/site-packages/google/cloud || true

# Fix jobmanager address in config.yaml to match our container name
RUN chmod 644 /opt/flink/conf/config.yaml && \
    sed -i 's/    address: jobmanager/    address: flink-jobmanager/' /opt/flink/conf/config.yaml

USER flink
