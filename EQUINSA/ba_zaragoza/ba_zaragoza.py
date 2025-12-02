import io
import re
import os
import requests
import datetime
from urllib.parse import quote_plus
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURACIÓN GENERAL ===
CREDENTIALS_FILE = 'credentials.json'
ENDPOINT_UPLOAD = "https://endpoints.caabsa.com/SucursalesSINUBE_API/uploadSINUBE_Formato_EQUINSA"
SUCURSAL_HEADER = "136 BA ZARAGOZA"

# === MAPA DE MESES EN ESPAÑOL MAYÚSCULAS ===
MESES_ES = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
}
# Mapa inverso: "AGOSTO" -> 8
MESES_INV = {v.upper(): k for k, v in MESES_ES.items()}

# === AUTENTICACIÓN CON GOOGLE DRIVE ===
def conectar_drive():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

# === BUSCAR CARPETA POR NOMBRE ===
def buscar_carpeta_id(service, nombre, parent_id=None):
    query = f"name = '{nombre}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    resultados = service.files().list(q=query, fields="files(id, name)").execute()
    archivos = resultados.get('files', [])
    if archivos:
        print(f"📁 Carpeta encontrada: {archivos[0]['name']}")
        return archivos[0]['id']
    else:
        print(f"❌ Carpeta '{nombre}' no encontrada.")
        return None

# === OBTENER NOMBRE DE ARCHIVO/CARPETA POR ID ===
def obtener_nombre_por_id(service, file_id):
    meta = service.files().get(fileId=file_id, fields="id, name").execute()
    return meta.get("name")

# === LISTAR ARCHIVOS XLSX QUE NO ESTÉN PROCESADOS ===
def listar_archivos_boletaje(service, folder_id):
    query = (
        f"'{folder_id}' in parents and trashed = false "
        f"and name contains '.xlsx' and not name contains '_procesado'"
    )
    resultados = service.files().list(q=query, fields="files(id, name, parents)").execute()
    return sorted(resultados.get('files', []), key=lambda x: x['name'])

# === DESCARGAR ARCHIVO ===
def descargar_archivo(service, file_id, nombre_destino):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"⬇️ Descargando... {int(status.progress() * 100)}%")
    with open(nombre_destino, 'wb') as f:
        f.write(fh.getvalue())
    print(f"✅ Archivo guardado localmente: {nombre_destino}")

# === SUBIR ARCHIVO AL ENDPOINT CON PARÁMETRO DE SUCURSAL ===
def subir_archivo(endpoint_base, archivo_path, sucursal_nombre):
    try:
        endpoint = f"{endpoint_base}?sucursal={quote_plus(sucursal_nombre)}"
        with open(archivo_path, 'rb') as f:
            files = {
                'file': (
                    os.path.basename(archivo_path),
                    f,
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            }
            response = requests.post(endpoint, files=files, verify=False, timeout=1200)
        print(f"📤 Código de respuesta: {response.status_code}")
        print(f"📄 Respuesta del servidor: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error al subir archivo: {e}")
        return False

# === RENOMBRAR ARCHIVO EN DRIVE ===
def renombrar_archivo(service, file_id, nuevo_nombre):
    try:
        service.files().update(fileId=file_id, body={"name": nuevo_nombre}).execute()
        print(f"📁 Archivo renombrado como: {nuevo_nombre}")
    except Exception as e:
        print(f"❌ Error al renombrar archivo: {e}")

# === MOVER ARCHIVO A RESPALDO ===
def mover_a_respaldo(service, file_id, from_id, to_id):
    try:
        service.files().update(
            fileId=file_id,
            addParents=to_id,
            removeParents=from_id,
            fields='id'
        ).execute()
        print("📁 Archivo movido a carpeta RESPALDO")
    except Exception as e:
        print(f"❌ Error al mover archivo a RESPALDO: {e}")

# === FLUJO PRINCIPAL ===
def main():
    hoy = datetime.datetime.now()
    anio = str(hoy.year)

    service = conectar_drive()

    # Navegación por carpetas
    raiz_id = buscar_carpeta_id(service, "Archivos de Carga Estacionamientos - ENTRA")
    plaza_id = buscar_carpeta_id(service, "14. 136-BA ZARAGOZA", raiz_id)
    anio_id = buscar_carpeta_id(service, anio, plaza_id)

    # ============================
    # Selección del MES (automático vs manual)
    # ============================
    USAR_MES_MANUAL = False      # ← pon True para pruebas
    MES_MANUAL = "AGOSTO"        # ← cuando USAR_MES_MANUAL=True, usa este folder

    if USAR_MES_MANUAL:
        mes_nombre = MES_MANUAL.strip().upper()
        mes_id = buscar_carpeta_id(service, mes_nombre, anio_id)
    else:
        mes_nombre_sistema = MESES_ES[hoy.month]  # automático por mes actual
        mes_id = buscar_carpeta_id(service, mes_nombre_sistema, anio_id)

    if not mes_id:
        print("🚫 No se encontró la carpeta del mes.")
        return

    # Leemos el NOMBRE REAL del folder de mes y lo convertimos a número MM
    nombre_mes_real = obtener_nombre_por_id(service, mes_id) or ""
    clave_mes = nombre_mes_real.strip().upper()
    if clave_mes not in MESES_INV:
        print(f"⚠️ No pude mapear el mes desde el folder '{nombre_mes_real}'. "
              f"Usaré el mes del sistema como fallback.")
        mes_num = hoy.month
    else:
        mes_num = MESES_INV[clave_mes]
    mes_dos = f"{mes_num:02d}"
    print(f"🧩 Mes detectado por carpeta: '{nombre_mes_real}' → prefijo '{mes_dos}_'")

    th_id = buscar_carpeta_id(service, "TH", mes_id)
    respaldo_id = buscar_carpeta_id(service, "RESPALDO", mes_id)

    if not th_id or not respaldo_id:
        print("🚫 No se encontró carpeta TH o RESPALDO.")
        return

    archivos = listar_archivos_boletaje(service, th_id)
    if not archivos:
        print("❌ No hay archivos .xlsx para procesar.")
        return

    for archivo in archivos:
        nombre_original = archivo['name']
        # Prefijo MM_ según el mes de la carpeta detectado arriba
        nombre_local = f"{mes_dos}_{nombre_original}"

        print(f"🔄 Procesando archivo: {nombre_original} → {nombre_local}")
        descargar_archivo(service, archivo['id'], nombre_local)

        if subir_archivo(ENDPOINT_UPLOAD, nombre_local, SUCURSAL_HEADER):
            # Renombrado en Drive: conserva nombre original + _procesado
            if nombre_original.lower().endswith(".xlsx"):
                nuevo_nombre_drive = nombre_original[:-5] + "_procesado.xlsx"
            else:
                nuevo_nombre_drive = nombre_original + "_procesado"
            renombrar_archivo(service, archivo['id'], nuevo_nombre_drive)
            mover_a_respaldo(service, archivo['id'], th_id, respaldo_id)
            try:
                os.remove(nombre_local)
                print("🗑️ Archivo local eliminado")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar el archivo local: {e}")
        print("")

if __name__ == '__main__':
    main()
