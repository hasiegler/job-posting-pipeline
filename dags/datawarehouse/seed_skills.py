"""
Seed script for skills in Supabase.
Run locally or from inside the Airflow container:
    set -a && source .env && set +a && python3 dags/datawarehouse/seed_skills.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from psycopg2.extras import execute_values

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor


SKILL_CATALOG = {
    "programming_language": [
        "Python",
        "JavaScript",
        "TypeScript",
        "Java",
        "SQL",
        "C++",
        "C#",
        "Go",
        "Rust",
        "Scala",
        "Ruby",
        "PHP",
        "Swift",
        "Kotlin",
        "R",
        "MATLAB",
        "Bash",
        "PowerShell",
        "Perl",
        "Objective-C",
    ],
    "cloud": [
        "AWS",
        "Azure",
        "GCP",
        "Snowflake",
        "Databricks",
        "Oracle Cloud",
        "IBM Cloud",
        "DigitalOcean",
        "Heroku",
        "Vercel",
        "Netlify",
        "Cloudflare",
        "Alibaba Cloud",
        "Salesforce",
        "ServiceNow",
    ],
    "database": [
        "PostgreSQL",
        "MySQL",
        "MongoDB",
        "Redis",
        "Cassandra",
        "DynamoDB",
        "Elasticsearch",
        "SQL Server",
        "Oracle Database",
        "SQLite",
        "MariaDB",
        "Couchbase",
        "Neo4j",
        "ClickHouse",
        "TimescaleDB",
        "InfluxDB",
        "CockroachDB",
        "Firestore",
        "BigQuery",
        "Redshift",
        "Teradata",
        "Greenplum",
        "Vertica",
        "Pinecone",
        "Weaviate",
    ],
    "big_data": [
        "Spark",
        "Hadoop",
        "Kafka",
        "Airflow",
        "Flink",
        "Hive",
        "Presto",
        "Trino",
        "Databricks",
        "dbt",
        "Fivetran",
        "Airbyte",
        "Prefect",
        "Dagster",
        "Snowflake",
        "BigQuery",
        "EMR",
        "DataFlow",
        "Glue",
        "Data Factory",
    ],
    "devops": [
        "Docker",
        "Kubernetes",
        "Terraform",
        "Ansible",
        "Jenkins",
        "GitLab CI",
        "GitHub Actions",
        "CircleCI",
        "ArgoCD",
        "Helm",
        "Prometheus",
        "Grafana",
        "Datadog",
        "New Relic",
        "Splunk",
        "ELK Stack",
        "Nginx",
        "Apache",
        "HAProxy",
        "Consul",
        "Vault",
        "Packer",
        "CloudFormation",
        "Pulumi",
        "Nomad",
    ],
    "web_framework": [
        "React",
        "Angular",
        "Vue",
        "Next.js",
        "Node.js",
        "Express",
        "Django",
        "Flask",
        "FastAPI",
        "Spring Boot",
        "Ruby on Rails",
        "Laravel",
        "ASP.NET",
        "Svelte",
        "Nuxt",
        "Gatsby",
        "Remix",
        "NestJS",
        "GraphQL",
        "gRPC",
    ],
    "data_science_library": [
        "Pandas",
        "NumPy",
        "Scikit-learn",
        "TensorFlow",
        "PyTorch",
        "Keras",
        "Matplotlib",
        "Seaborn",
        "SciPy",
        "Statsmodels",
        "XGBoost",
        "LightGBM",
        "Hugging Face",
        "LangChain",
        "OpenCV",
        "NLTK",
        "spaCy",
        "Transformers",
        "JAX",
        "MLflow",
    ],
    "bi_analytics": [
        "Tableau",
        "Power BI",
        "Looker",
        "Metabase",
        "Superset",
        "Qlik",
        "Sisense",
        "Domo",
        "Mode Analytics",
        "ThoughtSpot",
        "Google Analytics",
        "Mixpanel",
        "Amplitude",
        "Segment",
        "Jupyter",
    ],
    "version_control": [
        "Git",
        "GitHub",
        "GitLab",
        "Bitbucket",
        "Jira",
        "Confluence",
        "Slack",
        "Linear",
        "Asana",
        "Notion",
    ],
    "testing_quality": [
        "Jest",
        "Pytest",
        "JUnit",
        "Selenium",
        "Cypress",
        "Playwright",
        "Postman",
        "k6",
        "Locust",
        "SonarQube",
    ],
    "other": [
        "Excel",
        "Google Sheets",
        "VS Code",
        "IntelliJ",
        "PyCharm",
        "Figma",
        "Photoshop",
        "Illustrator",
        "Sketch",
        "Postman",
        "Insomnia",
        "Webpack",
        "Vite",
        "Babel",
        "ESLint",
    ],
}

SPECIAL_ALIASES = {
    "Python": ["python", "python3", "python 3", "py3", "py 3", "py"],
    "JavaScript": ["javascript", "js", "ecmascript"],
    "TypeScript": ["typescript", "ts"],
    "Java": ["java", "core java", "java se"],
    "SQL": ["sql", "structured query language", "ansi sql"],
    "Go": ["go", "golang", "go lang"],
    "R": ["r", "r language", "r programming"],
    "MATLAB": ["matlab", "mat lab", "mathworks matlab"],
    "Bash": ["bash", "shell scripting", "bash shell"],
    "PowerShell": ["powershell", "power shell", "pwsh"],
    "Objective-C": ["objective-c", "objective c", "objc"],
    "AWS": ["aws", "amazon web services"],
    "Azure": ["azure", "microsoft azure"],
    "GCP": ["gcp", "google cloud platform"],
    "C++": ["c++", "cpp"],
    "C#": ["c#", "csharp"],
    "Node.js": ["node.js", "nodejs"],
    "Next.js": ["next.js", "nextjs"],
    "ASP.NET": ["asp.net", "aspnet"],
    "Kubernetes": ["kubernetes", "k8s", "kube"],
    "Terraform": ["terraform", "tf", "hashicorp terraform"],
    "Git": ["git", "git scm", "git version control"],
    "GitHub": ["github", "git hub", "gh"],
    "GitLab": ["gitlab", "git lab", "gl"],
    "Postman": ["postman", "postman api", "postman collections"],
    "gRPC": ["grpc", "g-rpc"],
    "SQL Server": ["sql server", "mssql"],
    "PostgreSQL": ["postgresql", "postgres"],
    "MySQL": ["mysql", "my sql"],
    "MongoDB": ["mongodb", "mongo"],
    "Neo4j": ["neo4j", "neo 4j"],
    "dbt": ["dbt", "data build tool"],
    "EMR": ["emr", "elastic mapreduce"],
    "ELK Stack": ["elk stack", "elasticsearch logstash kibana"],
    "Nginx": ["nginx", "engine x"],
    "GitHub Actions": ["github actions", "gha"],
    "GitLab CI": ["gitlab ci", "gitlab-ci"],
    "Power BI": ["power bi", "powerbi"],
    "Jupyter": ["jupyter", "jupyter notebook"],
    "k6": ["k6", "k6 load testing"],
    "VS Code": ["vs code", "vscode"],
    "Scikit-learn": ["scikit-learn", "sklearn"],
    "NumPy": ["numpy", "np"],
    "PyTorch": ["pytorch", "torch"],
    "Hugging Face": ["hugging face", "hf"],
    "LangChain": ["langchain", "lang chain"],
}


def _build_aliases(skill_name):
    def _push(alias_list, value):
        value = (value or "").strip()
        if value and value not in alias_list:
            alias_list.append(value)

    aliases = []
    for alias in SPECIAL_ALIASES.get(skill_name, []):
        _push(aliases, alias.lower())

    normalized = skill_name.lower().strip()
    no_dots = normalized.replace(".", "")
    no_hyphen = normalized.replace("-", " ")
    no_slash = normalized.replace("/", " ")
    compact = "".join(ch for ch in normalized if ch.isalnum())
    amp_to_and = normalized.replace("&", " and ")
    amp_removed = normalized.replace("&", "")

    for alias in [
        normalized,
        no_dots.strip(),
        no_hyphen.strip(),
        no_slash.strip(),
        " ".join(no_slash.split()),
        compact,
        " ".join(amp_to_and.split()),
        amp_removed.strip(),
    ]:
        _push(aliases, alias)

    if len(aliases) < 2:
        _push(aliases, f"{normalized} tool")

    return aliases[:8]


def _build_skill_records():
    records = []
    for category, skills in SKILL_CATALOG.items():
        for skill_name in skills:
            records.append((skill_name, category, _build_aliases(skill_name)))
    return records


def seed_skills():
    conn, cur = get_conn_cursor()

    execute_values(cur, """
        INSERT INTO skills (skill_name, category, aliases)
        VALUES %s
        ON CONFLICT (skill_name, category)
        DO UPDATE SET
            aliases = EXCLUDED.aliases,
            is_active = TRUE,
            updated_at = NOW()
    """, _build_skill_records())

    conn.commit()
    print("skills table seeded successfully.")
    close_conn_cursor(conn, cur)


if __name__ == "__main__":
    seed_skills()
