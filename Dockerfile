FROM python:3.12-slim

COPY --from=apache/beam_python3.12_sdk:2.62.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python312-template-launcher-base:20250220-rc00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Implement get pipeline name/location from build
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Copy from project root
COPY requirements.txt .
COPY pyproject.toml .
COPY setup.py .

RUN pip install --no-cache-dir -r requirements.txt && \
    # explicit install of apache-beam to potentially reduce build time
    pip install -e . && \
    rm -rf /root/.cache/pip/*

# parameterize location
COPY main.py .

# parameterize location
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

ENTRYPOINT ["/opt/apache/beam/boot"]