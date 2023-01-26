from flask import Flask, jsonify, request
from neo4j import GraphDatabase
from constants import *
import uuid

app = Flask(__name__)

INITIALISE = False

EMPLOYEE = "Employee"
DEPARTMENT = "Department"

WORKS_IN = "WORKS_IN"
MANAGES = "MANAGES"

db = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test1234"))
if INITIALISE:
    with db.session() as session:
        session.run(f"""CREATE (a:{EMPLOYEE} {{name: "Marcin"}}),
        (b:{EMPLOYEE} {{name: "Ania", id: "{uuid.uuid4()}"}}),
        (c:{EMPLOYEE} {{name: "Maciek", id: "{uuid.uuid4()}"}}),
        (d:{EMPLOYEE} {{name: "Pulina", id: "{uuid.uuid4()}"}}),
        (e:{EMPLOYEE} {{name: "Marysia", id: "{uuid.uuid4()}"}}),
        (f:{DEPARTMENT} {{name: "Analiza Danych", id: "{uuid.uuid4()}"}}),
        (g:{DEPARTMENT} {{name: "Testowanie", id: "{uuid.uuid4()}"}}),
        (h:{DEPARTMENT} {{name: "Developer oprogramowania", id: "{uuid.uuid4()}"}}),
        (a)-[:{WORKS_IN}]->(f),
        (f)-[:{MANAGES}]->(a),
        (a)-[:{WORKS_IN}]->(g),
        (g)-[:{MANAGES}]->(a),
        (b)-[:{WORKS_IN}]->(h),
        (h)-[:{MANAGES}]->(b),
        (c)-[:{WORKS_IN}]->(h),
        (h)-[:{MANAGES}]->(c),
        (d)-[:{WORKS_IN}]->(f),
        (f)-[:{MANAGES}]->(d),
        (e)-[:{WORKS_IN}]->(g),
        (g)-[:{MANAGES}]->(e)
        """)


@app.route('/employees', methods=["GET"])
def get_employees_route():
    query = request.args
    sort = query.get("sort")
    filter = query.get("filter")
    value = query.get("value")
    print(filter)
    print(f"""MATCH (employee: {EMPLOYEE} {f"{{{filter}: '{value}'}}" if filter and value else ""})
        RETURN employee
        {"ORDER BY employee." + sort if sort else ""}""")
    with db.session() as session:
        result = session.run(f"""MATCH (employee: {EMPLOYEE}{f"{{name: '{filter}'}}" if filter else ""})
        RETURN employee
        {"ORDER BY employee." + sort if sort else ""}""")
        mapped_result = list(map(lambda x: x.data()["employee"], list(result)))
        print(mapped_result)
        return jsonify(mapped_result)


@app.route('/employees', methods=["POST"])
def post_employees_route():
    json_data = request.json
    with db.session() as session:
        result = session.run(f"""MATCH (employee: {EMPLOYEE} {{name: '{json_data["name"]}'}}) RETURN employee""")
        if not list(result):
            added_user = session.run(
                f"""CREATE (employee: {EMPLOYEE} {{name: '{json_data["name"]}', uuid: "{uuid.uuid4()}"}}) 
                RETURN employee""")
            return jsonify(added_user.single().data()["employee"])
    return {"err": "Pracownik istnieje"}


@app.route('/employees/<id>', methods=["PUT"])
def put_employee_route(id):
    json_data = request.json
    with db.session() as session:
        result = list(session.run(f"""MATCH (employee: {EMPLOYEE} {{id: "{id}"}}) RETURN employee"""))
        if not result:
            return {"err": "Pracownik nie istnieje"}
        employee = result[0].data()["employee"]
        for key in employee.keys():
            if not json_data.get(key):
                continue
            employee[key] = json_data[key]
        update_string = ""
        for row in employee.items():
            update_string += f"{row[0]}: '{row[1]}',"
        result = session.run(
            f"""MATCH (employee:{EMPLOYEE} {{id: "{id}"}}) SET employee += {{{update_string[:-1]}}} RETURN employee""")
        return jsonify(result.single().data()["employee"])


@app.route('/employees/<id>', methods=["DELETE"])
def delete_employee_route(id):
    with db.session() as session:
        result = list(session.run(
            f"""MATCH (n:Employee {{id: "{id}"}}) 
            OPTIONAL MATCH (n)-[r]->(:Department) RETURN collect(r) AS relations, n AS employee"""))
        if not result:
            return {"err": "Pracownik nie istnieje"}
        employee = result[0].data()
        for relation in employee["relations"]:
            if relation[1] == MANAGES:
                return "Ten pracownik zarządza departamentem"
        result = list(session.run(
            f"""MATCH (n:Employee {{id: "{id}"}}) DETACH DELETE n RETURN n AS employee"""))
        return jsonify(result[0].data()["employee"])


@app.route('/employees/<id>/subordinates', methods=["GET"])
def get_subordinates_route(id):
    with db.session() as session:
        result = list(session.run(f"""MATCH (e:{EMPLOYEE} {{id: "{id}"}})-[:MANAGES]->(d:{DEPARTMENT}) 
        WITH d, e
        MATCH (em:{EMPLOYEE} WHERE em <> e)-[:{WORKS_IN}]->(d)
        RETURN em"""))
        mapped_result = list(map(lambda x: x.data()["em"], result))
        return jsonify(mapped_result)


@app.route("/employees/<id>/department", methods=["GET"])
def get_department_summary_route(id):
    with db.session() as session:
        result = list(session.run(f"""
        MATCH (e:{EMPLOYEE} {{id: "{id}"}})-[:{WORKS_IN}]->(d:{DEPARTMENT}) WITH d
        MATCH (em:{EMPLOYEE})-[:{WORKS_IN}]->(d)
        OPTIONAL MATCH (m:{EMPLOYEE})-[:{MANAGES}]->(d)
        RETURN d.name as name, collect(em) as employees, m as manager
        """))
        print(result)
        mapped_result = []

        for record in result:
            mapped_record = record.data()
            mapped_result.append(mapped_record)
        return jsonify(mapped_result)


@app.route("/departments", methods=["GET"])
def get_departments():
    with db.session() as session:
        result = list(session.run(f"""MATCH (d:{DEPARTMENT}) RETURN d AS departments"""))
        mapped_result = list(map(lambda x: x.data(), result))
        return mapped_result


@app.route("/departments/<id>/employees", methods=["GET"])
def get_employees_in_department(id):
    with db.session() as session:
        result = list(session.run(f"""MATCH (d:{DEPARTMENT} {{id: "{id}"}})<-[:{WORKS_IN}]-(e:{EMPLOYEE}) RETURN e AS employees"""))
        mapped_result = list(map(lambda x: x.data(), result))
        return mapped_result


if __name__ == '__main__':
    app.run()