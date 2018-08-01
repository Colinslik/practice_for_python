def get_prediction_data(self, filter_status):
        prediction_data = []
        try:
            diskevent_conn = diskevent.DiskEvent('*')
            prediction_result = diskevent_conn.event_trigger()

            if prediction_result:
                for hostname, info in prediction_result.iteritems():
                    for diskname, diskstatus in info.iteritems():
                        status = diskstatus["near_failure"]
                        if status.lower() != "good" and \
                                (not filter_status or
                                 status.lower() == filter_status):
                            timestamp = int(diskstatus["time"]) / 1000000000
                            created_at = datetime.fromtimestamp(
                                timestamp
                            ).strftime('%Y-%m-%d %H:%M:%S')

                            prediction_data.append({
                                "created_at": created_at,
                                "host": hostname,
                                "disk": diskname,
                                "state": status,
                                "meta": {
                                    "id": diskstatus["time"],
                                    "timestamp": timestamp
                                }})
        except dpclient.DbError as e:
            print e.msg

        return prediction_data
