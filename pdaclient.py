class PdaApi(object):
    """
    PDA API client
    """

    def __init__(self):
        pass

    def update_backups(self, jobinfo):
        for host, info in jobinfo.iteritems():
            joblist = []
            url = "%s/%s/backups/%s" % (self.endpoint, platform.node(), host)
            for data in info:
                '''
                try:
                    datetime_object = datetime.strptime(
                        data["EXEC-TIME"], '%m/%d/%Y %H:%M:%S')
                    execTime = int(time.mktime(datetime_object.timetuple()))
                except Exception:
                    current_time = int(time.time())
                    execTime = current_time
                '''
                joblist.append({
                    "name": data["DESCRIPTION"],
                    "server": data["EXECUTIONHOST"],
                    "id": "" if data["JOBID"] == "0" else data["JOBID"],
                    "no": int(data["JOB#"]),
                    "status": data["STATUS"],
                    #"execTime": execTime,
                    "execTime": data["EXEC-TIME"],
                    "type": data["JOB-TYPE"],
                    "lastResult": data["LAST-RESULT"],
                    "owner": data["OWNER"]
                })

            params = {"backups": joblist}

            _logger.debug("[Pdaclient] url: %s, body: %s" % (url, params))
            status, data, _ = self._send_cmd(
                'PUT', url, params, [OK], json_content=True)

            if status not in [OK]:
                _logger.error("status: %s, results: %s" % (status, data))


if __name__ == '__main__':
    pass
