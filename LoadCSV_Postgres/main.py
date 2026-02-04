from sqlalchemy import create_engine, text
import pandas as pd
import os
import datetime

INPUT_PATH = '/Users/matheusdacostasanteago/Documents/healthcare_system_data'


def connect_to_db():
    """"
    Connect to PostgresSQL database
    """
    username, password, database =  os.getenv("POSTGRES_AIRFLOW_USER"), os.getenv("POSTGRES_AIRFLOW_PASSWORD"), 'postgres'

    try:
        engine = create_engine(f"postgresql+psycopg2://{username}:{password}@localhost:5432/{database}",
                               connect_args={'options': '-c search_path=healthcare'}
        )

        return engine
    except Exception as e:
        print(e)
        raise Exception

def get_filenames() -> list:
    """
    Get filenames at input path
    """
    return [{"filename": file.replace('.csv', ''), "full_path": f'{INPUT_PATH}/{file}', "modification_time": datetime.datetime.fromtimestamp(os.path.getmtime(f'{INPUT_PATH}/{file}'))} for file in os.listdir(INPUT_PATH) if not file.startswith('README')]

def update_checkpoint(table_name, ingestion_timestamp, engine):
    """
    Update checkpoint table
    """
    with engine.connect() as conn:
        query = text("""
                     INSERT INTO healthcare.checkpoints (table_name, last_checkpoint)
                     VALUES (:table, :ts) 
                     ON CONFLICT (table_name) 
                     DO UPDATE SET last_checkpoint = EXCLUDED.last_checkpoint;
                     """)

        conn.execute(query, {"table": table_name, "ts": ingestion_timestamp})
        conn.commit()

        print(f'✅Checkpoint updated to {table_name} table.\n')

def check_checkpoint(engine, filename, file_modification_timestamp):
    """
    Check if checkpoint for catch only new files.
    """
    try:
        with engine.connect() as conn:
            query = text("""SELECT last_checkpoint
                            FROM postgres.healthcare.checkpoints
                            WHERE table_name = :filename""")

            last_checkpoint = conn.execute(query, {"filename": filename}).fetchone()[0]

            if file_modification_timestamp < last_checkpoint:
                print(f'⏱️Checkpoint exists, getting files where checkpoint is greater than {last_checkpoint}.')
                return True
            else:
                print(f'❌ Any file with checkpoint greater than {last_checkpoint} was found in the database for table {filename}, skipping.')
                return False
    except Exception as e:
        if 'NoneType' in str(e):
            print(f'❌ Checkpoint file {filename} does not exist.')
            return True
        else:
            raise e


def load_db():
    """
    Load PostgreSQL database
    """
    engine = connect_to_db()

    for DATA_ARGS in get_filenames():
        ingestion_timestamp = datetime.datetime.now()
        filename = DATA_ARGS['filename']
        fullpath = DATA_ARGS['full_path']
        file_modification_timestamp = DATA_ARGS['modification_time']

        if check_checkpoint(engine, filename, file_modification_timestamp):
            print(f'⏳Starting load files from {fullpath} at {ingestion_timestamp}')

            df = pd.read_csv(fullpath)
            df['ingestion_timestamp'] = ingestion_timestamp

            if 'created_at' in df.columns:
                pass
            else:
                print(filename, "❌ Delta column doesn't exist\n")

            df.to_sql(filename, if_exists='replace', index=False, con=engine, schema='healthcare')
            print(f"✅Successfully loaded on postgres.healthcare.{filename}")
            update_checkpoint(filename, file_modification_timestamp, engine)
        else:
            pass


if __name__ == '__main__':
    load_db()