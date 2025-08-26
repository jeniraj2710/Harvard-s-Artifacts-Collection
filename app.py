import streamlit as st
import requests
import mysql.connector
import pandas as pd

# =========================
#  DB CONNECTION
# =========================
def get_connection():
    return mysql.connector.connect(
        host="gateway01.us-east-1.prod.aws.tidbcloud.com",
        port=4000,
        user="BmGZrKH9Ak35wQV.root",
        password="qyFTdfWQDSBsaN4d",
        database="Harvard_Artifacts"
    )

# =========================
#  CREATE TABLES
# =========================
def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artifact_metadata (
            id INT PRIMARY KEY,
            title TEXT,
            culture TEXT,
            period TEXT,
            century TEXT,
            medium TEXT,
            dimensions TEXT,
            description TEXT,
            department TEXT,
            classification TEXT,
            accessionyear INT,
            accessionmethod TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artifact_media (
            objectid INT,
            imagecount INT,
            mediacount INT,
            colorcount INT,
            media_rank INT,
            datebegin INT,
            dateend INT,
            FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artifact_colors (
            objectid INT,
            color TEXT,
            spectrum TEXT,
            hue TEXT,
            percent FLOAT,
            css3 TEXT,
            FOREIGN KEY(objectid) REFERENCES artifact_metadata(id)
        )
    """)

# =========================
#  FETCH DATA FROM API
# =========================
API_KEY = "98caeae1-8b72-42df-9664-a28ae2f90919re"  
BASE_URL = "https://api.harvardartmuseums.org/object"

def fetch_artifacts(classification, size=50, records=100):
    """Fetch artifacts by classification"""
    url = f"{BASE_URL}?classification={classification}&apikey={API_KEY}&size={size}"
    all_data = []

    while len(all_data) < records and url:
        res = requests.get(url)
        data = res.json()
        all_data.extend(data.get("records", []))
        url = data.get("info", {}).get("next")  # pagination

    return all_data[:records]

# =========================
#  INSERT INTO SQL
# =========================
def insert_data(cursor, artifacts):
    for art in artifacts:
        # Metadata
        cursor.execute("""
            INSERT IGNORE INTO artifact_metadata
            (id, title, culture, period, century, medium, dimensions, description, department, classification, accessionyear, accessionmethod)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            art.get("id"),
            art.get("title"),
            art.get("culture"),
            art.get("period"),
            art.get("century"),
            art.get("medium"),
            art.get("dimensions"),
            art.get("description"),
            art.get("department"),
            art.get("classification"),
            art.get("accessionyear"),
            art.get("accessionmethod")
        ))

        # Media
        cursor.execute("""
            INSERT INTO artifact_media
            (objectid, imagecount, mediacount, colorcount, media_rank, datebegin, dateend)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            art.get("id"),
            art.get("imagecount"),
            art.get("mediacount"),
            art.get("colorcount"),
            art.get("rank"),
            art.get("datebegin"),
            art.get("dateend")
        ))

        # Colors
        colors = art.get("colors") or []
        for c in colors:
            cursor.execute("""
                INSERT INTO artifact_colors
                (objectid, color, spectrum, hue, percent, css3)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                art.get("id"),
                c.get("color"),
                c.get("spectrum"),
                c.get("hue"),
                c.get("percent"),
                c.get("css3")
            ))

# =========================
#  PREDEFINED QUERIES
# =========================
queries = {
    "Artifacts from 11th century Byzantine": 
        "SELECT * FROM artifact_metadata WHERE century='11th century' AND culture='Byzantine';",
    "Unique cultures": 
        "SELECT DISTINCT culture FROM artifact_metadata;",
    "Artifacts from Archaic Period": 
        "SELECT * FROM artifact_metadata WHERE period='Archaic Period';",
    "Artifacts by accession year (desc)": 
        "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC;",
    "Artifacts per department": 
        "SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department;"
}

# =========================
#  STREAMLIT UI
# =========================
st.title("🏛️ Harvard Artifacts Explorer")

# --- Sidebar Controls ---
st.sidebar.header("Controls")
classification = st.sidebar.text_input("Classification", "Coins")
records = st.sidebar.slider("Number of Records", 100, 2500, 200)

# --- Collect Data ---
if st.sidebar.button("Collect Data"):
    st.write(f"Fetching {records} records for '{classification}'...")
    data = fetch_artifacts(classification, records=records)
    st.session_state["data"] = data
    st.success(f"Fetched {len(data)} records")

# --- Show Data ---
if st.sidebar.button("Show Data"):
    if "data" in st.session_state:
        st.dataframe(pd.DataFrame(st.session_state["data"]).head(20))
    else:
        st.warning("⚠️ No data fetched yet!")

# --- Insert into SQL ---
if st.sidebar.button("Insert into SQL"):
    if "data" in st.session_state:
        conn = get_connection()
        cur = conn.cursor()
        create_tables(cur)
        insert_data(cur, st.session_state["data"])
        conn.commit()
        conn.close()
        st.success("✅ Data inserted into SQL")
    else:
        st.warning("⚠️ No data to insert!")

# --- Run Predefined Queries ---
st.subheader("🔍 Explore Data with Predefined Queries")
query_choice = st.selectbox("Choose a query", list(queries.keys()))

if st.button("Run Query"):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute(queries[query_choice])
    rows = cur.fetchall()
    conn.close()

    if rows:
        st.dataframe(pd.DataFrame(rows))
    else:
        st.info("No results found")
