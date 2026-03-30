SECRET_KEY = "changeme-insecure-dev-key"
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset@postgres:5432/superset"
WTF_CSRF_ENABLED = True
ROW_LIMIT = 5000
SQLLAB_CTAS_NO_LIMIT = True
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
