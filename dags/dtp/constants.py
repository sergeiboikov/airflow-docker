from airflow.models import Variable
from ast import literal_eval
from datetime import datetime

# Common project constants
PRJ_NAME = "dtpprj"

# Constants for Airflow
AIRFLOW_DAG_START_DATE = datetime(2024, 1, 1)

# Constants for Web
WEB_DTP_CONN_ID = "web_dtp"
WEB_RETRIES = int(Variable.get("dtpprj_v_web_retries",
                               default_var=2))
WEB_RETRY_DELAY_SEC = int(Variable.get("dtpprj_v_web_retry_delay_sec",
                                       default_var=120))

# Constants for S3
S3_CONN_ID = "minio_s3"
S3_BUCKET = "open-datasets"

DTPPRJ_V_DAG__DTPPRJ__LOAD_WEB_S3_DTP__REGIONS = \
    literal_eval(Variable.get("dtpprj_v_dag.dtpprj__load_web_s3_dtp.regions",
                              default_var='''(
                              "altaiskii-krai",
                              "amurskaia-oblast",
                              "arkhangelskaia-oblast",
                              "astrakhanskaia-oblast",
                              "belgorodskaia-oblast",
                              "brianskaia-oblast",
                              "vladimirskaia-oblast",
                              "volgogradskaia-oblast",
                              "vologodskaia-oblast",
                              "voronezhskaia-oblast",
                              "evreiskaia-avtonomnaia-oblast",
                              "zabaikalskii-krai",
                              "ivanovskaia-oblast",
                              "irkutskaia-oblast",
                              "kabardino-balkarskaia-respublika",
                              "kaliningradskaia-oblast",
                              "kaluzhskaia-oblast",
                              "kamchatskii-krai",
                              "karachaevo-cherkesskaia-respublika",
                              "kemerovskaia-oblast-kuzbass",
                              "kirovskaia-oblast",
                              "kostromskaia-oblast",
                              "krasnodarskii-krai",
                              "krasnoiarskii-krai",
                              "kurganskaia-oblast",
                              "kurskaia-oblast",
                              "leningradskaia-oblast",
                              "lipetskaia-oblast",
                              "magadanskaia-oblast",
                              "moskva",
                              "murmanskaia-oblast",
                              "nenetskii-avtonomnyi-okrug",
                              "nizhegorodskaia-oblast",
                              "novgorodskaia-oblast",
                              "novosibirskaia-oblast",
                              "omskaia-oblast",
                              "orenburgskaia-oblast",
                              "orlovskaia-oblast",
                              "penzenskaia-oblast",
                              "permskii-krai",
                              "primorskii-krai",
                              "pskovskaia-oblast",
                              "respublika-adygeia-adygeia",
                              "respublika-altai",
                              "respublika-bashkortostan",
                              "respublika-buriatiia",
                              "respublika-dagestan",
                              "respublika-ingushetiia",
                              "respublika-kalmykiia",
                              "respublika-kareliia",
                              "respublika-komi",
                              "respublika-krym",
                              "respublika-marii-el",
                              "respublika-mordoviia",
                              "respublika-sakha-iakutiia",
                              "respublika-severnaia-osetiia-alaniia",
                              "respublika-tatarstan-tatarstan",
                              "respublika-tyva",
                              "respublika-khakasiia",
                              "rostovskaia-oblast",
                              "riazanskaia-oblast",
                              "samarskaia-oblast",
                              "sankt-peterburg",
                              "saratovskaia-oblast",
                              "sakhalinskaia-oblast",
                              "sverdlovskaia-oblast",
                              "sevastopol",
                              "smolenskaia-oblast",
                              "stavropolskii-krai",
                              "tambovskaia-oblast",
                              "tverskaia-oblast",
                              "tomskaia-oblast",
                              "tulskaia-oblast",
                              "tiumenskaia-oblast",
                              "udmurtskaia-respublika",
                              "ulianovskaia-oblast",
                              "khabarovskii-krai",
                              "khanty-mansiiskii-avtonomnyi-okrug-iugra",
                              "cheliabinskaia-oblast",
                              "chechenskaia-respublika",
                              "chuvashskaia-respublika-chuvashiia",
                              "chukotskii-avtonomnyi-okrug",
                              "iamalo-nenetskii-avtonomnyi-okrug",
                              "iaroslavskaia-oblast")'''))
