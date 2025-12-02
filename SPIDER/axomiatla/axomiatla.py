import io
import os
import re
import datetime
import requests
from urllib.parse import quote_plus
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURACIÓN GENERAL ===
CREDENTIALS_FILE = 'credentials.json'
# Se sube al endpoint con el nombre prefijado MM_ (ej. 09_nombre.xlsx)
ENDPOINT_UPLOAD = "https://endpoints.caabsa.com/SucursalesSINUBE_API/uploadSINUBE_Formato_SPIDER"
SUCURSAL_HEADER = "14 AXIOMIATLA"

# === MAPA DE MESES EN ESPAÑOL MAYÚSCULAS ===
MESES_ES = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
}
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

# === OBTENER NOMBRE (archivo/carpeta) POR ID ===
def obtener_nombre_por_id(service, file_id):
    meta = service.files().get(fileId=file_id, fields="id, name").execute()
    return meta.get("name")

# === LISTAR XLSX/XLS/CSV/GOOGLE SHEETS QUE NO ESTÉN PROCESADOS ===
def listar_archivos_boletaje(service, folder_id):
    q = (
        f"'{folder_id}' in parents and trashed = false "
        f"and (mimeType='application/vnd.google-apps.spreadsheet' "
        f"or name contains '.xlsx' or name contains '.xls' or name contains '.csv') "
        f"and not name contains '_procesado'"
    )
    resultados = service.files().list(
        q=q,
        fields="files(id, name, parents, mimeType)"
    ).execute()
    archivos = resultados.get('files', [])
    return sorted(archivos, key=lambda x: x['name'].lower())

# === DESCARGAR / EXPORTAR ARCHIVO ===
def descargar_archivo(service, file_id, nombre_drive, mime_type):
    # Limpia nombre para el filesystem
    safe_name = re.sub(r'[\\/:*?\"<>|]+', '_', nombre_drive).strip()

    # Si es Google Sheets -> exportar a XLSX
    if mime_type == 'application/vnd.google-apps.spreadsheet':
        base, _ext = os.path.splitext(safe_name)
        nombre_destino = base + ".xlsx"
        print("📝 Hoja de cálculo de Google detectada. Exportando a .xlsx…")
        data = service.files().export(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ).execute()
        with open(nombre_destino, 'wb') as f:
            f.write(data)
        print(f"✅ Archivo exportado: {nombre_destino}")
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return nombre_destino, content_type

    # Si es archivo normal (xlsx/xls/csv/otro descargable)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"⬇️ Descargando... {int(status.progress() * 100)}%")
    with open(safe_name, 'wb') as f:
        f.write(fh.getvalue())
    print(f"✅ Archivo guardado localmente: {safe_name}")

    # Content-type según extensión
    ext = os.path.splitext(safe_name)[1].lower()
    if ext == ".csv":
        content_type = "text/csv"
    elif ext == ".xlsx":
        content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif ext == ".xls":
        content_type = "application/vnd.ms-excel"
    else:
        content_type = "application/octet-stream"
    return safe_name, content_type

# === SUBIR ARCHIVO AL ENDPOINT CON PARÁMETRO DE SUCURSAL ===
def subir_archivo(endpoint_base, archivo_path, sucursal_nombre, content_type_hint=None):
    try:
        endpoint = f"{endpoint_base}?sucursal={quote_plus(sucursal_nombre)}"
        if content_type_hint is None:
            ext = os.path.splitext(archivo_path)[1].lower()
            if ext == ".csv":
                content_type_hint = "text/csv"
            elif ext == ".xlsx":
                content_type_hint = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif ext == ".xls":
                content_type_hint = "application/vnd.ms-excel"
            else:
                content_type_hint = "application/octet-stream"

        with open(archivo_path, 'rb') as f:
            files = {'file': (os.path.basename(archivo_path), f, content_type_hint)}
            response = requests.post(endpoint, files=files, verify=False, timeout=1200)
        print(f"📤 Código de respuesta: {response.status_code}")
        print(f"📄 Respuesta del servidor: {response.text[:800]}{'…' if len(response.text)>800 else ''}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error al subir archivo: {e}")
        return False

# === (NO USADO) RENOMBRAR ARCHIVO EN DRIVE ===
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

    # Navegación por carpetas (ACTUALIZADO)
    raiz_id = buscar_carpeta_id(service, "Archivos de carga Estacionamientos - ENTRA")
    plaza_id = buscar_carpeta_id(service, "8. 014-AXOMIATLA", raiz_id)
    anio_id = buscar_carpeta_id(service, anio, plaza_id)

    # ============================
    # Selección del MES (automático vs manual)
    # ============================
    USAR_MES_MANUAL = False       # ← pon True para pruebas
    MES_MANUAL = "SEPTIEMBRE"        # ← cuando USAR_MES_MANUAL=True, usa este folder

    if USAR_MES_MANUAL:
        mes_nombre = MES_MANUAL.strip().upper()
        mes_id = buscar_carpeta_id(service, mes_nombre, anio_id)
    else:
        mes_nombre_sistema = MESES_ES[hoy.month]  # automático por mes actual
        mes_id = buscar_carpeta_id(service, mes_nombre_sistema, anio_id)

    if not mes_id:
        print("🚫 No se encontró carpeta del mes.")
        return

    # Prefijo MM_ desde el NOMBRE REAL de la carpeta del mes
    nombre_mes_real = obtener_nombre_por_id(service, mes_id) or ""
    clave_mes = nombre_mes_real.strip().upper()
    if clave_mes not in MESES_INV:
        print(f"⚠️ No pude mapear el mes desde el folder '{nombre_mes_real}'. Uso mes del sistema.")
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
        print("❌ No hay archivos .xlsx/.xls/.csv/Sheets para procesar.")
        return

    for archivo in archivos:
        nombre_original = archivo['name']
        mime_type = archivo.get('mimeType', '')
        print(f"🔄 Procesando archivo: {nombre_original} ({mime_type})")

        # 1) Descargar / Exportar
        ruta_local, content_type = descargar_archivo(service, archivo['id'], nombre_original, mime_type)
        if not ruta_local:
            print("⏭️ Omitido por tipo no soportado o error.")
            continue

        # 2) Prefijar con MM_ el archivo local (sin tocar Drive)
        nombre_prefijado = f"{mes_dos}_{os.path.basename(ruta_local)}"
        if nombre_prefijado != ruta_local:
            try:
                os.rename(ruta_local, nombre_prefijado)
                ruta_local = nombre_prefijado
                print(f"🏷️ Renombrado local: {ruta_local}")
            except Exception as e:
                print(f"⚠️ No se pudo renombrar con prefijo MM_: {e}")

        # 3) Subir (con nombre prefijado)
        ok = subir_archivo(ENDPOINT_UPLOAD, ruta_local, SUCURSAL_HEADER, content_type_hint=content_type)

        # 4) Si subió OK, mover a RESPALDO **sin renombrar en Drive**
        if ok:
            mover_a_respaldo(service, archivo['id'], th_id, respaldo_id)
            try:
                os.remove(ruta_local)
                print("🗑️ Archivo local eliminado")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar el archivo local: {e}")
        else:
            print("⚠️ No se movió a RESPALDO porque la subida falló.")
        print("")

if __name__ == '__main__':
    main()
