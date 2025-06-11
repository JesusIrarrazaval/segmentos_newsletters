
# Dependencias
import os
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
from datetime import datetime, timedelta
import hashlib
import boto3
import pytz
import schedule
import time
from pathlib import Path
from dotenv import load_dotenv

pd.options.mode.chained_assignment = None  # quitar warnings de pandas

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv('accessKeyId'),
    aws_secret_access_key=os.getenv('secretAccessKey'),
    region_name=os.getenv('region'),
)
XXXXXXXXXXX = boto3.client(
    "XXXXXXXXXXX",
    aws_access_key_id=os.getenv('accessKeyId'),
    aws_secret_access_key=os.getenv('secretAccessKey'),
    region_name=os.getenv('region'),
)

test = False

Path("archivos").mkdir(parents=True, exist_ok=True)

### Definiendo variables para banners publicitarios diferenciados en newsletters ###

# Definiendo enlaces para Attributes.tipo_usuario_img

link_suscriptor_img = "https://s3-XXXXXXXXXX.s3.us-east-1.amazonaws.com/xxxxxx/imagen1.png"
link_registrado_img = "https://s3-XXXXXXXXXX.s3.us-east-1.amazonaws.com/xxxxxx/imagen2.png"

# Definiendo enlaces para Attributes.tipo_usuario_url (usar '#' cuando no se requiera un enlace)

link_suscriptor_url = "#" 
link_registrado_url = "https://suscripciondigital.latercera.com/"

### Definiendo las variables de los newsletters ###

newsletters = [
    # La variable Attributes.newsletter debe coincidir con el nombre de la columna respectiva en la tabla XXXXXXXXXXX.segmentos
    ##################################### LOS DATOS FUERON CENSURADOS POR POLÍTICAS DE PRIVACIDAD ######################################
    {
        "nombre_base": "base 1",
        "nombre_columna": "col1",
        "plataforma": "XXXXXXXXXXX",
        "app_id": "id_app_1",
        "segment_id": "id_segment_1",
        "Attributes.newsletter": "attribute1",
    },
    {
        "nombre_base": "base 2",
        "nombre_columna": "col2",
        "plataforma": "XXXXXXXXXXX",
        "app_id": "app_id_2",
        "segment_id": "segment_id_2",
        "Attributes.newsletter": "attribute2",
    },
    {
        "nombre_base": "base 3",
        "nombre_columna": "col3",
        "plataforma": "XXXXXXXXXXX",
        "app_id": "app_id_3",
        "segment_id": "segment_id_3",
        "Attributes.newsletter": "attribute3",
    },
    
    #### LA LISTA OFICIAL CONTIENE MÁS NEWSLETTERS, PERO POR SIMPLICIDAD, SE HA RESUMIDO HASTA ACÁ ####
]

def main():
    inicio = time.time()

    print("==> Conectándose a Redshift")
    conn = psycopg2.connect(
        host=os.getenv('host'),
        port=os.getenv('port'),
        database=os.getenv('database'),
        user=os.getenv('user'),
        password=os.getenv('password'),
    )
    cursor = conn.cursor()

    print("==> Verificando si la automatización 'XXXXXXXXXX' se ejecutó hoy")

    query = """
    SELECT ultima_actualizacion
    FROM automatizaciones.registro_automatizaciones
    WHERE nombre = 'XXXXXXXXXX';
    """
    estado_XXXXXXXXXX = sqlio.read_sql_query(query, conn)

    if len(estado_XXXXXXXXXX) == 0:
        print("Error: No se encontró la automatización 'XXXXXXXXXX' en la tabla de registros.")
        cursor.close()
        conn.close()
        return "Automatización detenida"
    
    ultima_actualizacion_XXXXXXXXXX = estado_XXXXXXXXXX.iloc[0]["ultima_actualizacion"].tz_convert("America/Santiago")

    if ultima_actualizacion_XXXXXXXXXX.strftime("%Y-%m-%d") != datetime.now().strftime("%Y-%m-%d"):
        print("La automatización 'XXXXXXXXXX' no se ha ejecutado hoy. Saliendo del proceso.")
        cursor.close()
        conn.close()
        return "Automatización detenida"

    print("La automatización 'XXXXXXXXXX' se ejecutó hoy. Continuando con el proceso...")

    print("==> Verificando el estado de esta automatización")
    query = f"""
  SELECT ultima_actualizacion
  FROM automatizaciones.registro_automatizaciones
  WHERE nombre = 'Bases Newsletters';
  """
    automatizacion = sqlio.read_sql_query(query, conn)

    if len(automatizacion) > 0:
        ultima_actualizacion = automatizacion.iloc[0][
            "ultima_actualizacion"
        ].tz_convert("America/Santiago")
        if ultima_actualizacion.strftime("%Y-%m-%d") == datetime.now().strftime(
            "%Y-%m-%d"
        ):
            if test == False:
                cursor.close()
                conn.close()
                print("La automatización ya fue ejecutada hoy")
                return "ya se ejecutó"
    
    print("==> Actualizando desinscritos")
    query = "CREATE TABLE IF NOT EXISTS XXXXXXXXXXX.desinscritos (email VARCHAR(1000) UNIQUE);"
    cursor.execute(query)
    conn.commit()

    # Obtener desinscritos registrados previamente
    query = """
    SELECT email
    FROM XXXXXXXXXXX.desinscritos;
    """
    desinscritos_previos = sqlio.read_sql_query(query, conn)

    # Obtener desinscritos logs
    query = """
    SELECT DISTINCT(LOWER(destination)) AS email
    FROM XXXXXXXXXXX.email_unsubscribe;
    """
    desinscritos_logs = sqlio.read_sql_query(query, conn)

    desinscritos_nuevos = desinscritos_logs[
        ~(desinscritos_logs.email.isin(desinscritos_previos.email))
    ]

    if len(desinscritos_nuevos) > 0:
        s3.put_object(
            Body=desinscritos_nuevos.to_csv(index=False),
            Bucket="bucket",
            Key=f"archivo.csv",
        )

        query = f"""
        COPY XXXXXXXXXXX.desinscritos
        FROM 's3://XXXXXXXXXXXXXX/archivo.csv'
        CREDENTIALS 'aws_access_key_id={os.getenv('accessKeyId')};aws_secret_access_key={os.getenv('secretAccessKey')}'
        IGNOREHEADER AS 1
        DELIMITER ','
        MAXERROR 0
        FILLRECORD;
        """
        cursor.execute(query)
        conn.commit()

        s3.delete_object(
            Bucket="bucket", 
            Key="archivo.csv"
        )

    print(f"desinscritos previos: {len(desinscritos_previos)}")
    print(f"desinscritos nuevos: {len(desinscritos_nuevos)}")

    desinscritos = pd.concat(
        [desinscritos_previos, desinscritos_nuevos]
    )
    print(f"desinscritos: {len(desinscritos)}")

    print("==> Actualizando registros hardbounces")
    query = (
        "CREATE TABLE IF NOT EXISTS XXXXXXXXXXX.hardbounces (email VARCHAR(1000) UNIQUE);"
    )
    cursor.execute(query)
    conn.commit()

    # Obtener hardbounces registrados previamente
    query = """
  SELECT email
  FROM XXXXXXXXXXX.hardbounces;
  """
    hardbounces_previos = sqlio.read_sql_query(query, conn)

    # Obtener hardbounces logs
    query = """
  SELECT DISTINCT(LOWER(destination)) AS email
  FROM XXXXXXXXXXX.hardbounce
  WHERE event_timestamp >= 'YYYY-MM-DD';
  """
    hardbounces_logs = sqlio.read_sql_query(query, conn)

    hardbounces_nuevos = hardbounces_logs[
        ~(hardbounces_logs.email.isin(hardbounces_previos.email))
    ]

    if len(hardbounces_nuevos) > 0:
        s3.put_object(
            Body=hardbounces_nuevos.to_csv(index=False),
            Bucket="bucket",
            Key="archivo.csv",
        )

        query = f"""
      COPY XXXXXXXXXXX.hardbounces
      FROM 's3://XXXXXXXXXXXXXX/archivo.csv'
      CREDENTIALS 'aws_access_key_id={os.getenv('accessKeyId')};aws_secret_access_key={os.getenv('secretAccessKey')}'
      IGNOREHEADER AS 1
      DELIMITER ','
      MAXERROR 0
      FILLRECORD;
      """
        cursor.execute(query)
        conn.commit()

        s3.delete_object(
            Bucket="bucket",
            Key="archivo.csv",
        )

    print(f"hardbounces previos: {len(hardbounces_previos)}")
    print(f"hardbounces nuevos: {len(hardbounces_nuevos)}")

    hardbounces = pd.concat([hardbounces_previos, hardbounces_nuevos])
    hardbounces["hb"] = 1
    print(f"hardbounces: {len(hardbounces)}")

    print("==> Actualizando registros no molestar")
    query = (
        "CREATE TABLE IF NOT EXISTS XXXXXXXXXXX.no_molestar (email VARCHAR(1000) UNIQUE);"
    )
    cursor.execute(query)
    conn.commit()

    # Obtener no molestar registrados previamente
    query = """
  SELECT email
  FROM XXXXXXXXXXX.no_molestar;
  """
    no_molestar_previos = sqlio.read_sql_query(query, conn)

    # Obtener no molestar excel
    s3.download_file(
        Bucket="bucket",
        Key="archivo.csv",
        Filename="archivos/archivo.xlsx",
    )

    no_molestar_excel = pd.read_excel(
        os.path.join("archivos", "archivo.xlsx"),
        sheet_name="Correos",
        engine="openpyxl",
    )
    os.remove("archivos/archivo.xlsx")
    no_molestar_excel.rename({"Correo": "email"}, axis=1, inplace=True)
    no_molestar_excel.drop_duplicates(subset=["email"], inplace=True, ignore_index=True)
    no_molestar_excel["email"] = no_molestar_excel["email"].str.lower()
    no_molestar_excel = no_molestar_excel[["email"]]

    no_molestar_nuevos = no_molestar_excel[
        ~(no_molestar_excel.email.isin(no_molestar_previos.email))
    ]

    if len(no_molestar_nuevos) > 0:
        s3.put_object(
            Body=no_molestar_nuevos.to_csv(index=False),
            Bucket="bucket",
            Key="archivo.csv",
        )

        query = f"""
      COPY XXXXXXXXXXX.no_molestar
      FROM 's3://XXXXXXXXXXXXXX/archivo.csv'
      CREDENTIALS 'aws_access_key_id={os.getenv('accessKeyId')};aws_secret_access_key={os.getenv('secretAccessKey')}'
      IGNOREHEADER AS 1
      DELIMITER ','
      MAXERROR 0
      FILLRECORD;
      """
        cursor.execute(query)
        conn.commit()

        s3.delete_object(
            Bucket="bucket",
            Key="archivo.csv",
        )

    print(f"no molestar previos: {len(no_molestar_previos)}")
    print(f"no molestar nuevos: {len(no_molestar_nuevos)}")

    no_molestar = pd.concat([no_molestar_previos, no_molestar_nuevos])
    no_molestar["nm"] = 1
    print(f"no molestar: {len(no_molestar)}")

    print("==> Actualizando tabla de segmentos newsletters")
    query = f'SELECT * FROM XXXXXXXXXXX.segmentos{"_test" if test == True else ""};'
    segmentos = sqlio.read_sql_query(query, conn)

    # print(query)

    # 1
    # Obtenemos a los suscriptores
    query = """
        SELECT
          LOWER(TRIM(c.email)) AS email
        FROM sistema.contactos AS c
        JOIN sistema.socios AS s
          ON c.socio_id = s.id
        JOIN sistema.lineas_suscripcion AS l
          ON c.socio_id = l.socio_id
        JOIN sistema.cabeceras_suscripcion AS cb
          ON l.cabecera_id = cb.id
        WHERE c.email NOT LIKE ''
          AND c.email LIKE '%@%'
          AND c.activo = 'Y'
          AND c.predeterminado = 'Y'
          AND c.tipo_contacto = '03'
          AND s.tipo_persona IN ('N', 'M')
          AND l.producto LIKE '%PRODUCTO%'
          AND cb.estado = 'CO'
          AND cb.status = 'ACTIVO'
          AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
          AND c.email NOT IN (SELECT email FROM sistema.emails_bounce)
          AND c.email NOT IN (SELECT email FROM sistema.emails_bloqueados)
        GROUP BY LOWER(TRIM(c.email));
    """

    suscriptores_totales = sqlio.read_sql_query(query, conn)

    # 2
    suscriptores_nuevos_base = suscriptores_totales[
        ~(suscriptores_totales.email.isin(segmentos.email))
    ]

    # 3
    segmentos = pd.concat([segmentos, suscriptores_nuevos_base])

    # 4
    # Seteamos en False a los usuarios que no sean suscriptores
    segmentos.loc[
        ~segmentos.email.isin(suscriptores_totales.email), "col_n"
    ] = False

    # Obtenemos a los suscriptores nuevos del día anterior
    query = """
        SELECT
          LOWER(TRIM(c.email)) AS email
        FROM sistema.contactos AS c
        JOIN sistema.socios AS s
          ON c.socio_id = s.id
        JOIN sistema.lineas_suscripcion AS l
          ON c.socio_id = l.socio_id
        JOIN sistema.cabeceras_suscripcion AS cb
          ON l.cabecera_id = cb.id
        WHERE c.email NOT LIKE ''
          AND c.email LIKE '%@%'
          AND c.activo = 'Y'
          AND c.predeterminado = 'Y'
          AND c.tipo_contacto = '03'
          AND s.tipo_persona IN ('N', 'M')
          AND l.producto LIKE '%PRODUCTO%'
          AND cb.estado = 'CO'
          AND cb.status = 'ACTIVO'
          AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
          AND DATE(cb.fecha_activacion) = current_date - INTEGER '1'
          AND c.email NOT IN (SELECT email FROM sistema.emails_bounce)
          AND c.email NOT IN (SELECT email FROM sistema.emails_bloqueados)
        GROUP BY LOWER(TRIM(c.email));
    """
    suscriptores_nuevos = sqlio.read_sql_query(query, conn)

    if test == True:
        print(f"{len(suscriptores_nuevos)} suscriptores nuevos")

    # Si la automatización nunca ha sido ejecutada, entonces dejamos a todos los suscriptores inscritos en el EA. De lo contrario, sólo lo hacemos con los suscriptores nuevos del día anterior.
    if len(automatizacion) == 0:
        segmentos.loc[
            segmentos.email.isin(suscriptores_totales.email),
            "col_n",
        ] = True
    else:
        segmentos.loc[
            segmentos.email.isin(suscriptores_nuevos.email),
            "col_n",
        ] = True

    # Obtenemos a todos los suscriptores de CPC
    query = """
        SELECT
          LOWER(TRIM(c.email)) AS email
        FROM sistema.contactos AS c
        JOIN sistema.socios AS s
          ON c.socio_id = s.id
        JOIN sistema.lineas_suscripcion AS l
          ON c.socio_id = l.socio_id
        JOIN sistema.cabeceras_suscripcion AS cb
          ON l.cabecera_id = cb.id
        WHERE c.email NOT LIKE ''
          AND c.email LIKE '%@%'
          AND c.activo = 'Y'
          AND c.predeterminado = 'Y'
          AND c.tipo_contacto = '03'
          AND s.tipo_persona IN ('N', 'M')
          AND l.producto LIKE '%PRODUCTO_2%'
          AND cb.estado = 'CO'
          AND cb.status = 'ACTIVO'
          AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
        GROUP BY LOWER(TRIM(c.email));
    """

    suscriptores_cpc_totales = sqlio.read_sql_query(query, conn)
    # 2
    suscriptores_cpc_nuevos_base = suscriptores_totales[
        ~(suscriptores_totales.email.isin(segmentos.email))
    ]

    # 3
    segmentos = pd.concat(
        [segmentos, suscriptores_cpc_nuevos_base]
    )

    # 4
    # Seteamos en False a los usuarios que no sean suscriptores de CPC
    segmentos.loc[
        ~segmentos.email.isin(suscriptores_cpc_totales.email),
        "col_n",
    ] = False

    # Obtenemos a los suscriptores nuevos del día anterior
    query = """
        SELECT
          LOWER(TRIM(c.email)) AS email
        FROM sistema.contactos AS c
        JOIN sistema.socios AS s
          ON c.socio_id = s.id
        JOIN sistema.lineas_suscripcion AS l
          ON c.socio_id = l.socio_id
        JOIN sistema.cabeceras_suscripcion AS cb
          ON l.cabecera_id = cb.id
        WHERE c.email NOT LIKE ''
          AND c.email LIKE '%@%'
          AND c.activo = 'Y'
          AND c.predeterminado = 'Y'
          AND c.tipo_contacto = '03'
          AND s.tipo_persona IN ('N', 'M')
          AND l.producto LIKE '%PRODUCTO_2%'
          AND cb.estado = 'CO'
          AND cb.status = 'ACTIVO'
          AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
          AND l.fecha_desde = 'YYYY-MM-DD'
        GROUP BY LOWER(TRIM(c.email));
    """

    suscriptores_cpc_nuevos = sqlio.read_sql_query(query, conn)

    if test == True:
        print(f"{len(suscriptores_cpc_nuevos)} suscriptores nuevos")

    # Si la automatización nunca ha sido ejecutada, entonces dejamos a todos los suscriptores inscritos en el EA. De lo contrario, sólo lo hacemos con los suscriptores nuevos del día anterior.
    segmentos.loc[
        segmentos.email.isin(suscriptores_cpc_totales.email),
        "col_n",
    ] = True


    # Obtenemos a los socios de XXXXXXXXX
    query = """
        SELECT DISTINCT(LOWER(TRIM(c.email))) AS email
        FROM sistema.contactos AS c
        WHERE c.activo = 'Y'
          AND c.predeterminado = 'Y'
          AND c.tipo_contacto = '03'
          AND c.email NOT LIKE '% %'
          AND c.email LIKE '%@%'
          AND c.email NOT LIKE '%,%'
          AND c.rut_socio_negocio IN (

          SELECT DISTINCT s.rut
          FROM sistema.socios AS s
          WHERE s.tipo_persona IN ('N', 'M')
            AND s.rut NOT LIKE '%@%'
            AND s.id IN (
              SELECT l.socio_id
              FROM sistema.lineas_suscripcion AS l
              LEFT JOIN sistema.cabeceras_suscripcion AS cb
              ON l.cabecera_id = cb.id
              WHERE l.producto LIKE '%PRODUCTO%'
                AND cb.estado = 'CO'
                AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
                AND l.fecha_desde < CURRENT_DATE
                AND l.fecha_hasta >= CURRENT_DATE
            )
          GROUP BY s.rut

          UNION

          SELECT DISTINCT(r.relacionado_rut)
          FROM sistema.relaciones_socios AS r
          LEFT JOIN sistema.socios AS s ON r.relacionado_rut = s.rut
          LEFT JOIN sistema.lineas_suscripcion AS l ON r.rut_socio = l.rut_socio
          WHERE s.tipo_persona IN ('N', 'M')
            AND r.beneficiario = 'Y'
            AND r.relacionado_rut NOT IN (
              SELECT l.rut_socio
              FROM sistema.lineas_suscripcion AS l
              LEFT JOIN sistema.cabeceras_suscripcion AS cb ON l.cabecera_id = cb.id
              WHERE l.producto LIKE '%PRODUCTO%'
                AND cb.estado = 'CO'
                AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
                AND l.fecha_desde < CURRENT_DATE
                AND l.fecha_hasta >= CURRENT_DATE
            )
            AND r.rut_socio IN (
              SELECT l.rut_socio
              FROM sistema.lineas_suscripcion AS l
              LEFT JOIN sistema.cabeceras_suscripcion AS cb ON l.cabecera_id = cb.id
              WHERE l.producto LIKE '%PRODUCTO%'
                AND cb.estado = 'CO'
                AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
                AND l.fecha_desde < CURRENT_DATE
                AND l.fecha_hasta >= CURRENT_DATE
            )
          GROUP BY r.relacionado_rut

          UNION

          SELECT DISTINCT(s.rut)
          FROM sistema.socios AS s
          LEFT JOIN sistema.cabeceras_suscripcion AS cb
            ON s.id = cb.id_socio_contratante
          LEFT JOIN sistema.lineas_suscripcion AS l
            ON cb.numero_documento = l.numero_documento
          WHERE s.tipo_persona IN ('N', 'M')
            AND cb.tipo_suscripcion <> 'TIPO_EXCLUIDO'
            AND s.id IN (
              SELECT DISTINCT(cb1.id_socio_contratante)
              FROM sistema.lineas_suscripcion AS l1
              LEFT JOIN sistema.cabeceras_suscripcion AS cb1
              ON l1.cabecera_id = cb1.id
              WHERE (l1.producto LIKE '%PRODUCTO%' OR l1.producto LIKE '%PRODUCTO_2%')
                AND cb1.estado = 'CO'
                AND cb1.tipo_suscripcion <> 'TIPO_EXCLUIDO'
                AND l1.fecha_desde < CURRENT_DATE
                AND l1.fecha_hasta >= CURRENT_DATE
            )
            AND s.rut IS NOT NULL
            AND s.rut NOT LIKE '% %'
            AND s.rut NOT LIKE '%RUT_RESERVADO%'
          GROUP BY s.rut
        )
        AND LOWER(TRIM(c.email)) NOT IN (
          SELECT email FROM sistema.emails_bounce
          UNION
          SELECT email FROM sistema.emails_bloqueados
          UNION
          SELECT email FROM sistema.segmentos
          WHERE beneficio_activo = false
            AND fecha_beneficio IS NOT NULL
        );

  """

    socios_totales = sqlio.read_sql_query(query, conn)
    socios_nuevos_base = socios_totales[
        ~(socios_totales.email.isin(segmentos.email))
    ]

    if test == True:
        print(f"socios totales: {len(socios_totales)}")
        print(f"socios nuevos base = {len(socios_nuevos_base)}")

    segmentos = pd.concat([segmentos, socios_nuevos_base])

    # Si el usuario no se encuentra en el objeto socios_totales lo sacamos del newsletter XXXXXXXXXXX. En caso contrario, lo agregamos al newsletter.
    segmentos.loc[
        ~segmentos.email.isin(socios_totales.email), "col_n"
    ] = False

    segmentos.loc[
        segmentos.email.isin(socios_totales.email), "col_n"
    ] = True

    segmentos = pd.merge(
        segmentos, hardbounces, how="left", on="email"
    )
    segmentos = pd.merge(
        segmentos, no_molestar, how="left", on="email"
    )
    segmentos["hardbounce"] = segmentos.apply(
        lambda x: True if x.hb == 1 else x.hardbounce, axis=1
    )
    segmentos["desinscrito_no_molestar"] = segmentos.apply(
        lambda x: True if x.nm == 1 else x.desinscrito_no_molestar, axis=1
    )
    segmentos = segmentos.stack(dropna=False)
    segmentos[segmentos == True] = 1
    segmentos[segmentos == False] = 0
    segmentos = segmentos.unstack()
    segmentos = segmentos.drop(["hb", "nm"], axis=1)

    # Limpiamos la tabla
    query = (
        f'DELETE FROM XXXXXXXXXXX.segmentos{"_test" if test == True else ""};'
    )
    cursor.execute(query)
    conn.commit()

    segmentos.to_csv("archivos/archivo.csv", index=False)

    s3.put_object(
        Body=segmentos.to_csv(index=False),
        Bucket="bucket",
        Key="archivo.csv",
    )

    query = f"""
  COPY XXXXXXXXXXX.segmentos{"_test" if test == True else ""}
  FROM 's3://XXXXXXXXXXXXXX/archivo.csv'
  CREDENTIALS 'aws_access_key_id={os.getenv('accessKeyId')};aws_secret_access_key={os.getenv('secretAccessKey')}'
  IGNOREHEADER AS 1
  DELIMITER ','
  MAXERROR 0
  FILLRECORD;
  """
    cursor.execute(query)
    conn.commit()

    s3.delete_object(
        Bucket="bucket",
        Key="archivo.csv",
    )

    print("==> Actualizando bases newsletters")
    base_esp = segmentos.copy()
    columnas_base_esp = [col for col in base_esp.columns if "fecha" not in col]
    base_esp = base_esp[columnas_base_esp]

    for newsletter in newsletters:
        print(f'Actualizando {newsletter["nombre_base"]} en XXXXXXXXXXX')
        fecha_hora = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        includeId = hashlib.md5(
            (fecha_hora + newsletter["nombre_base"]).encode("utf-8")
        ).hexdigest()
        # print(includeId)
        base = segmentos[
            [
                "email",
                newsletter["nombre_columna"],
                "hardbounce",
                "desinscrito_no_molestar",
            ]
        ]
        base = base.loc[
            (base["hardbounce"] != 1)
            & (base["desinscrito_no_molestar"] != 1)
            & (base[newsletter["nombre_columna"]] == 1)
        ]

        base["ChannelType"] = "EMAIL"
        base["Attributes.md5"] = [
            hashlib.md5(val.encode("UTF-8")).hexdigest() for val in base["email"]
        ]
        base["Attributes.tipo_usuario_img"] = base["email"].isin(suscriptores_totales["email"]).map({
            True: link_suscriptor_img,
            False: link_registrado_img
        })
        base["Attributes.tipo_usuario_url"] = base["email"].isin(suscriptores_totales["email"]).map({
            True: link_suscriptor_url,
            False: link_registrado_url
        })
        base["Attributes.includeId"] = includeId
        base["Attributes.newsletter"] = newsletter["Attributes.newsletter"]
        base = base[["ChannelType", "email", "Attributes.md5", "Attributes.includeId", 
                     "Attributes.newsletter", "Attributes.tipo_usuario_img", "Attributes.tipo_usuario_url"]]
        base.rename({"email": "Address"}, axis=1, inplace=True)

        # Subimos el segmento a S3
        s3.put_object(
            Body=base.to_csv(index=False),
            Bucket="bucket",
            Key="archivo.csv",
        )

        # Actualizamos los endpoints en el proyecto
        importJob = XXXXXXXXXXX.create_import_job(
            ApplicationId=newsletter["app_id"],
            ImportJobRequest={
                "DefineSegment": False,
                "Format": "CSV",
                "RegisterEndpoints": True,
                "RoleArn": "RoleArn",
                "S3Url": f's3://XXXXXXXXXX/endpoints {newsletter["nombre_base"]}.csv',
            },
        )

        jobId = importJob["ImportJobResponse"]["Id"]
        jobStatus = importJob["ImportJobResponse"]["JobStatus"]

        while jobStatus != "COMPLETED":
            jobInfo = XXXXXXXXXXX.get_import_job(
                ApplicationId=newsletter["app_id"], JobId=jobId
            )
            jobStatus = jobInfo["ImportJobResponse"]["JobStatus"]

        # Actualizamos el segmento dinámico
        XXXXXXXXXXX.update_segment(
            ApplicationId=newsletter["app_id"],
            SegmentId=newsletter["segment_id"],
            WriteSegmentRequest={
                "Dimensions": {
                    "Attributes": {
                        "includeId": {
                            "AttributeType": "INCLUSIVE",
                            "Values": [includeId],
                        }
                    }
                },
                "Name": f'{"NO USAR - " if test == True else ""}{datetime.utcnow().strftime("%Y-%m-%d")} {newsletter["nombre_base"]}',
            },
        )

        s3.delete_object(
            Bucket="bucket",
            Key="archivo.csv",
        )

    
    print("==> Actualizando newsletter XXXXXXX")
  
    base_XXXXXXX = pd.DataFrame({
        'ChannelType': ['EMAIL'] * len(suscriptores_totales),
        'email': suscriptores_totales['email']
    })
    fecha_hora = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    includeId = hashlib.md5((fecha_hora + 'base').encode('utf-8')).hexdigest()

    base_XXXXXXX['Attributes.md5'] = [hashlib.md5(val.encode('UTF-8')).hexdigest() for val in base_XXXXXXX['email']]
    base_XXXXXXX['Attributes.includeId'] = includeId
    base_XXXXXXX = base_XXXXXXX[['ChannelType', 'email', 'Attributes.md5', 'Attributes.includeId']]
    base_XXXXXXX.rename({'email': 'Address'}, axis=1, inplace=True)

    # Subimos el segmento a S3
    s3.put_object(
    Body = base_XXXXXXX.to_csv(index = False),
    Bucket="bucket",
    Key="archivo.csv",
    )

    # Actualizamos los endpoints en el proyecto
    importJob = XXXXXXXXXXX.create_import_job(
    ApplicationId='app_id_XXXXXXX',
        ImportJobRequest={
        'DefineSegment': False,
        'Format': 'CSV',
        'RegisterEndpoints': True,
        'RoleArn': 'RoleArn',
        'S3Url': 's3://XXXXXXXXX/base.csv'
        }
    )

    jobId = importJob['ImportJobResponse']['Id']
    jobStatus = importJob['ImportJobResponse']['JobStatus']

    while jobStatus != 'COMPLETED':
        jobInfo = XXXXXXXXXXX.get_import_job(
            ApplicationId='app_id',
            JobId=jobId
        )
        jobStatus = jobInfo['ImportJobResponse']['JobStatus']

    # Actualizamos el segmento dinámico
    XXXXXXXXXXX.update_segment(
        ApplicationId='app_id',
        SegmentId='segment_id',
        WriteSegmentRequest={
            'Dimensions': {
            'Attributes': {
                'includeId': {
                'AttributeType': 'INCLUSIVE',
                'Values': [includeId]
                }
            }
            },
            'Name': f'{"NO USAR - " if test == True else ""}{datetime.utcnow().strftime("%Y-%m-%d")} Newsletter Editorial'
        }
    )

    s3.delete_object(
        Bucket="bucket",
        Key="archivo.csv",
    )

    print("==> Actualizando registro de automatización")
    # primero verificamos si la automatización ya fue registrada previamente

    ultima_actualizacion = pytz.timezone("America/Santiago").localize(
        datetime.today().replace(microsecond=0)
    )
    tiempo_ejecucion = time.strftime("%H:%M:%S", time.gmtime(time.time() - inicio))
    proxima_actualizacion = pytz.timezone("America/Santiago").localize(
        datetime.now().replace(hour=3, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    )

    cursor.execute("LOCK automatizaciones.registro_automatizaciones;")
    conn.commit()

    if len(automatizacion) == 0:
        query = f"""
      INSERT INTO automatizaciones.registro_automatizaciones
          (
          nombre,
          lenguaje,
          ultima_actualizacion,
          tiempo_ejecucion,
          frecuencia_actualizacion,
          proxima_actualizacion
          )
          VALUES
          (
          'Bases Newsletters',
          'Python',
          '{ultima_actualizacion}',
          '{tiempo_ejecucion}',
          'Diaria',
          '{proxima_actualizacion}'
          );
      """
    else:
        query = f"""
      UPDATE automatizaciones.registro_automatizaciones
          SET ultima_actualizacion = '{ultima_actualizacion}',
              tiempo_ejecucion = '{tiempo_ejecucion}',
              proxima_actualizacion = '{proxima_actualizacion}'
      WHERE nombre = \'Bases Newsletters\';
      """

    cursor.execute(query)
    conn.commit()

    cursor.close()
    conn.close()

    print(
        f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} Finalizado en {tiempo_ejecucion}'
    )


if __name__ == '__main__':
    main()
    # schedule.every(1).minutes.do(main)
    schedule.every().day.at("05:20:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
