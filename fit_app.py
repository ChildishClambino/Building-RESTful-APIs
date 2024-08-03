from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error


app = Flask(__name__)
ma = Marshmallow(app)

class MemberSchema(ma.Schema):
    name = fields.String(required=True)
    age = fields.Integer(required=True)

    class Meta:
        fields= ("name", "age", "id")

member_schema = MemberSchema()
members_schema = MemberSchema(many=True)


class SessionSchema(ma.Schema):
    session_id = fields.Integer(required=True)
    member_id = fields.Integer(required=True)
    date = fields.Date(required=True)
    session_time = fields.String(required=True)
    activity = fields.String(required=True)
    
    class Meta:
        fields= ("member_id", "date", "session_time", "activity")

session_schema = SessionSchema()
sessions_schema = SessionSchema(many=True)

def get_db_connection():
    db_name = "assignment_sql"
    user = "root"
    password = "Like214!"
    host= "localhost"

    try:
        conn = mysql.connector.connect(
            database = db_name,
            user = user,
            password = password,
            host = host
        )

        print("Connected to MySQueaL Database Successfully!!!")
        return conn
    
    except Error as e:
        print(f"Error!: {e}")
        return None


@app.route('/')
def home():
    return 'Welcome to the Fitness Center Database App'

@app.route('/members', methods=["GET"])
def view_members():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED"}), 500
        
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM members"

        cursor.execute(query)

        members = cursor.fetchall()

        return members_schema.jsonify(members)
    
    except Error as e:
        print(f"Error!: {e}")
        return jsonify({"Error": "Internal Service Error"}), 500
    
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()



@app.route('/sessions', methods=["GET"])
def view_sessions():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED"}), 500
        
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM workoutsessions"

        cursor.execute(query)

        sessions = cursor.fetchall()

        return sessions_schema.jsonify(sessions)
    
    except Error as e:
        print(f"Error!: {e}")
        return jsonify({"Error": "Internal Service Error"}), 500
    
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()            



@app.route("/members", methods=["POST"])
def add_member():
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error!: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED!"}), 500
        cursor = conn.cursor()

        new_member = (member_data["id"], member_data["name"], member_data["age"])

        query = "INSERT INTO members (id, name, age) VALUES (%s, %s, %s)"

        cursor.execute(query, new_member)
        conn.commit()

        return jsonify({"message": "New Member Added Successfully!"}), 201
    except Error as e:
        print(f"Error!: {e}")

        return jsonify({"Error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()



@app.route("/sessions", methods=["POST"])
def add_session():
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error!: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED!"}), 500
        cursor = conn.cursor()

        new_session = (session_data["member_id"], session_data["date"], session_data["session_time"], session_data["activity"])

        query = "INSERT INTO workoutsessions (member_id, date, session_time, activity) VALUES (%s, %s, %s, %s)"

        cursor.execute(query, new_session)
        conn.commit()

        return jsonify({"message": "New Work Out Added Successfully!"}), 201
    except Error as e:
        print(f"Error!: {e}")

        return jsonify({"Error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()



@app.route("/members/<int:id>", methods=["PUT"])
def update_member(id):
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error!: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED!"}), 500
        cursor = conn.cursor()

        updated_member = (member_data["name"], member_data["age"], id)

        query = "UPDATE members SET name = %s, age = %s WHERE id = %s"

        cursor.execute(query, updated_member)
        conn.commit()

        return jsonify({"message": "Updated Member Successfully!"}), 201
    except Error as e:
        print(f"Error!: {e}")

        return jsonify({"Error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


@app.route("/sessions/<int:session_id>", methods=["PUT"])
def update_session(session_id):
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error!: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED!"}), 500
        cursor = conn.cursor()

        updated_session = (session_data["member_id"], session_data["date"], session_data["session_time"], session_data["activity"], session_id)

        query = "UPDATE workoutsessions SET member_id = %s, date = %s, session_time = %s, activity = %s WHERE  session_id = %s"

        cursor.execute(query, updated_session)
        conn.commit()

        return jsonify({"message": "Updated Member Successfully!"}), 201
    except Error as e:
        print(f"Error!: {e}")

        return jsonify({"Error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


@app.route("/members/<int:id>", methods=["DELETE"])
def delete_member(id):
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error!": "DATABASE CONNECTION FAILED!"}), 500
        cursor = conn.cursor()

        member_to_delete = (id,)

        cursor.execute('SELECT * FROM members WHERE id = %s', member_to_delete)
        member = cursor.fetchone()
        if not member:
            return jsonify({"Error": "Member not found!"}), 404

        query = "DELETE FROM members WHERE id = %s"

        cursor.execute(query, member_to_delete)
        conn.commit()

        return jsonify({"message": "Deleted Member Successfully!"}), 201
    except Error as e:
        print(f"Error!: {e}")

        return jsonify({"Error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


if  __name__ == '__main__':
    app.run(debug=True)