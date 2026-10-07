FROM hadoop-hive-spark-master

USER root
COPY --from=ghcr.io/astral-sh/uv:0.11.29 /uv /uvx /usr/local/bin/
RUN mv /usr/local/sbin/run.sh /usr/local/sbin/hadoop-run.sh \
    && mkdir -p /opt/data-refiner-api /opt/data-refiner-bootstrap \
    && sed -i \
        's|org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider|org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory|' \
        /opt/hive/conf/hive-site.xml \
    && grep -q 'SQLStdHiveAuthorizerFactory' /opt/hive/conf/hive-site.xml \
    && chown -R jupyter:jupyter /opt/data-refiner-api /opt/data-refiner-bootstrap

USER jupyter
ENV UV_PROJECT_ENVIRONMENT=/opt/data-refiner-api/.venv \
    UV_PYTHON_INSTALL_DIR=/home/jupyter/.local/share/uv/python
WORKDIR /opt/data-refiner-api
COPY --chown=jupyter:jupyter services/data-refiner-api/ ./
RUN uv python install 3.11 \
    && uv sync --locked --no-dev

COPY --chown=jupyter:jupyter deploy/bootstrap/ /opt/data-refiner-bootstrap/
COPY --chown=jupyter:jupyter services/data-refiner-user-workspace/run_cluster_args.py /opt/data-refiner-bootstrap/
COPY --chown=jupyter:jupyter deploy/master/run.sh /usr/local/sbin/run.sh

USER root
RUN chmod 755 /usr/local/sbin/run.sh /opt/data-refiner-bootstrap/bootstrap.sh
USER jupyter

EXPOSE 7778
WORKDIR /opt/data-refiner-api
CMD ["/usr/local/sbin/run.sh"]
