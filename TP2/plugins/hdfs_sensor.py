from airflow.sensors.base import BaseSensorOperator
import requests


class HdfsFileSensor(BaseSensorOperator):
    template_fields = ("hdfs_path", "namenode_url")

    def __init__(self, hdfs_path: str, namenode_url: str, **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.namenode_url = namenode_url

    def poke(self, context) -> bool:
        try:
            resp = requests.get(
                f"{self.namenode_url}/webhdfs/v1{self.hdfs_path}",
                params={"op": "GETFILESTATUS", "user.name": "root"},
                timeout=5,
            )

            if resp.status_code == 200:
                size = resp.json()["FileStatus"]["length"]
                self.log.info("Fichier trouvé : %s (%d bytes)", self.hdfs_path, size)
                return True

            self.log.info("Fichier non trouvé pour le moment : %s", self.hdfs_path)
            return False

        except Exception as exc:
            self.log.warning("Fichier absent ou HDFS indisponible : %s", exc)
            return False