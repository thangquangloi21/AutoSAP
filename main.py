from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from WorkThread import WorkThread
from Log import Logger


app = Flask(__name__)
LAST_REQUEST_TIME = None
LAST_REQUEST_TIME2 = None
LAST_REQUEST_TIME3 = None
LIMIT = timedelta(minutes=30)
LIMIT2 = timedelta(minutes=30)
LIMIT3 = timedelta(seconds=30)
RUNING = False

log = Logger()
WorkThread = WorkThread()


# Health check
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "Flask API",
        "version": "1.0"
    })


# POST example
@app.route("/api/echo", methods=["POST"])
def echo():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    return jsonify({
        "received": data
    })



@app.route("/api/loaddata", methods=["POST"])
def LoadSAPData():
    log.info("Update dữ liệu PBI")
    global LAST_REQUEST_TIME3, RUNING
    now = datetime.now()
    CheckStatus = WorkThread.Check_Status("SAP")
    
    if CheckStatus != "error" and LAST_REQUEST_TIME3 and now - LAST_REQUEST_TIME3 < LIMIT3:
        wait = LIMIT3 - (now - LAST_REQUEST_TIME3)
        log. info(f"Chưa thực hiện được thực hiện lại sau {str(wait.total_seconds())}")
        return jsonify({
            "error": "Too many requests",
            "retry_after_seconds": int(wait.total_seconds())
            }), 429

    LAST_REQUEST_TIME3 = now

    if RUNING:
        return jsonify({
            "error": "System is run"
            }), 429
    # update dữ liệu ở đây
    RUNING = True
    status = WorkThread.Load_Data()
        
    if not status:
        WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('SAP','error')")
        RUNING = False
        return jsonify({
            "error": "Export erorr",
        }), 429
        
    # TODO: refresh data + update SQL
    now = datetime.now()
    
    RUNING = False
    WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('SAP','{now.strftime("%H:%M %d-%m-%Y")}')")
    return jsonify({
        "status": "done",
        "time": now.strftime("%Y-%m-%d %H:%M:%S")
    })


# Start server
if __name__ == "__main__":
    log.info("Khởi động Flask API trên cổng 6666")
    app.run(
        host="0.0.0.0",  # cho phép máy khác truy cập
        port=6666,
        debug=True
    )
    log.info("Exit Flask API trên cổng 6666")
