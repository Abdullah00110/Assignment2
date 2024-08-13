from flask import Flask, request, redirect, url_for, flash, render_template
import pandas as pd
import mysql.connector
from mysql.connector import Error


app = Flask(__name__)
app.config['SECRET_KEY'] = '899c442d811fc0fe'


app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'root'
app.config['MYSQL_DB'] = 'climate_change_db'

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=app.config['MYSQL_HOST'],
            user=app.config['MYSQL_USER'],
            password=app.config['MYSQL_PASSWORD'],
            database=app.config['MYSQL_DB']
        )
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part', 'error')
        return redirect(url_for('index'))

    file = request.files['file']

    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(url_for('index'))

    if file and file.filename.endswith('.xlsx'):
        try:
            # Read the file into a Pandas DataFrame
            df = pd.read_excel(file)

            # Print the actual column names for debugging
            print("Actual columns in the file:", df.columns.tolist())

            # Define expected columns
            expected_columns = [
                'Country', 'Variable', 'Period', 'Annual', 'Jan', 'Feb', 'Mar', 'Apr',
                'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
            ]

            # Clean column names
            df.columns = df.columns.str.strip()

            # Check if all expected columns are in the DataFrame
            if not all(col in df.columns for col in expected_columns):
                flash('Column names in the file do not match the expected columns.', 'error')
                return redirect(url_for('index'))

            # Rename columns if needed
            df.rename(columns={
                # Update with actual old column names if needed
                'OldCountryName': 'Country',
                'OldVariableName': 'Variable',
            }, inplace=True)

            # Insert DataFrame records one by one into the database
            connection = get_db_connection()
            if connection:
                try:
                    cursor = connection.cursor()
                    for i, row in df.iterrows():
                        values = (
                            row['Country'], row['Variable'], row['Period'], row['Annual'],
                            row['Jan'], row['Feb'], row['Mar'], row['Apr'],
                            row['May'], row['Jun'], row['Jul'], row['Aug'],
                            row['Sep'], row['Oct'], row['Nov'], row['Dec']
                        )
                        sql = """
                        INSERT INTO climate_data (country, variable, period, annual, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, `dec`)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(sql, values)

                    connection.commit()
                    flash('Data successfully added to the database!', 'success')
                except Error as e:
                    flash(f'An error occurred: {e}', 'error')
                finally:
                    cursor.close()
                    connection.close()

            return redirect(url_for('index'))

        except Exception as e:
            flash(f'An error occurred while processing the file: {e}', 'error')
            return redirect(url_for('index'))

    flash('File format not supported', 'error')
    return redirect(url_for('index'))

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
