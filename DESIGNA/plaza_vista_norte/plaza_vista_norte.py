import io
import os
import re
import datetime
import mimetypes
import requests
from pathlib import Path
from urllib.parse import quote_plus
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURACIÓN GENERAL ===
CREDENTIALS_FILE = 'credentials.json'
ENDPOINT_UPLOAD = "https://endpoints.caabsa.com/SucursalesSINUBE_API/uploadSINUBE_183VistaNorte"
SUCURSAL_HEADER = "183 VISTA NORTE"

# === MAPA DE MESES EN ESPAÑOL MAYÚSCULAS ===
MESES_ES = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE"
}
# Inverso: "AGOSTO" -> 8
MESES_INV = {v.upper(): k for k, v in MESES_ES.items()}

# === EXPORTS SOPORTADOS PARA MIMETYPES DE GOOGLE ===
GOOGLE_EXPORTS = {
    'application/vnd.google-apps.spreadsheet': (
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'
    ),
    'application/vnd.google-apps.document': (
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx'
    ),
    'application/vnd.google-apps.presentation': (
        'application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx'
    ),
    'application/vnd.google-apps.drawing': (
        'image/png', '.png'
    ),
    'application/vnd.google-apps.jam': (
        'application/pdf', '.pdf'
    ),
}

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

# === OBTENER NOMBRE POR ID (archivo/carpeta) ===
def obtener_nombre_por_id(service, file_id):
    meta = service.files().get(fileId=file_id, fields="id, name").execute()
    return meta.get("name")

# === LISTAR TODOS LOS ARCHIVOS (EXCEPTO FOLDERS Y _procesado) CON PAGINACIÓN ===
def listar_archivos(service, folder_id):
    q = (
        f"'{folder_id}' in parents and trashed = false "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and not name contains '_procesado'"
    )
    page_token = None
    archivos = []
    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType, parents)",
            pageToken=page_token
        ).execute()
        archivos.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break
    return sorted(archivos, key=lambda x: x['name'].lower())

# === DESCARGAR O EXPORTAR ARCHIVO ===
def descargar_archivo(service, file_id, file_name, mime_type):
    # Limpia nombre
    safe_name = re.sub(r'[\\/:*?"<>|]+', '_', file_name).strip()

    # Archivos de Google: exportar
    if mime_type.startswith('application/vnd.google-apps.'):
        if mime_type in GOOGLE_EXPORTS:
            export_mime, ext = GOOGLE_EXPORTS[mime_type]
            base, _old_ext = os.path.splitext(safe_name)
            nombre_destino = base + ext
            print(f"📝 Archivo de Google detectado. Exportando como {ext}…")
            request = service.files().export(fileId=file_id, mimeType=export_mime)
            data = request.execute()
            with open(nombre_destino, 'wb') as f:
                f.write(data)
            print(f"✅ Archivo exportado: {nombre_destino}")
            return nombre_destino, export_mime
        else:
            print(f"⚠️ Tipo de Google no soportado para exportar: {mime_type}. Omitiendo.")
            return None, None
    # Archivos “normales”: descargar
    nombre_destino = safe_name
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
    guessed, _ = mimetypes.guess_type(nombre_destino)
    content_type = guessed or 'application/octet-stream'
    return nombre_destino, content_type

# === SUBIR ARCHIVO AL ENDPOINT CON PARÁMETRO DE SUCURSAL ===
def subir_archivo(endpoint_base, archivo_path, sucursal_nombre, content_type=None):
    try:
        endpoint = f"{endpoint_base}?sucursal={quote_plus(sucursal_nombre)}"
        if content_type is None:
            guessed, _ = mimetypes.guess_type(archivo_path)
            content_type = guessed or 'application/octet-stream'
        with open(archivo_path, 'rb') as f:
            files = {'file': (os.path.basename(archivo_path), f, content_type)}
            response = requests.post(endpoint, files=files, verify=False, timeout=1200)
        print(f"📤 Código de respuesta: {response.status_code}")
        preview = response.text if len(response.text) < 800 else response.text[:800] + '…'
        print(f"📄 Respuesta del servidor: {preview}")
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error al subir archivo: {e}")
        return False

# === RENOMBRAR ARCHIVO EN DRIVE (agrega _procesado antes de la extensión) ===
def renombrar_archivo(service, file_id, current_name):
    try:
        base, ext = os.path.splitext(current_name)
        nuevo_nombre = f"{base}_procesado{ext}"
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
        print("📦 Archivo movido a carpeta RESPALDO")
    except Exception as e:
        print(f"❌ Error al mover archivo a RESPALDO: {e}")

# === FLUJO PRINCIPAL ===
def main():
    hoy = datetime.datetime.now()
    anio = str(hoy.year)
    service = conectar_drive()

    # Navegación por carpetas
    raiz_id = buscar_carpeta_id(service, "Archivos de Carga Estacionamientos - ENTRA")
    plaza_id = buscar_carpeta_id(service, "98. 183-PLAZA VISTA NORTE", raiz_id)
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

    # Prefijo MM_ a partir del NOMBRE REAL de la carpeta del mes
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

    archivos = listar_archivos(service, th_id)
    if not archivos:
        print("ℹ️ No hay archivos para procesar (o todos ya tienen _procesado).")
        return

    for archivo in archivos:
        nombre = archivo['name']
        file_id = archivo['id']
        mime_type = archivo.get('mimeType', '')
        print(f"\n🔄 Procesando: {nombre}  ({mime_type})")

        # 1) Descargar/Exportar
        local_path, content_type = descargar_archivo(service, file_id, nombre, mime_type)
        if not local_path:
            print("⏭️ Omitido por tipo no soportado o error al exportar.")
            continue

        # 2) Prefijar con MM_ el archivo local (sin tocar el de Drive)
        prefixed_path = f"{mes_dos}_{os.path.basename(local_path)}"
        if prefixed_path != local_path:
            try:
                os.rename(local_path, prefixed_path)
                local_path = prefixed_path
                print(f"🏷️ Renombrado local: {local_path}")
            except Exception as e:
                print(f"⚠️ No se pudo renombrar a prefijo MM_: {e}")

        # 3) Subir
        ok = subir_archivo(ENDPOINT_UPLOAD, local_path, SUCURSAL_HEADER, content_type)

        # 4) Si subió bien: renombrar en Drive (agregar _procesado) y mover
        if ok:
            renombrar_archivo(service, file_id, nombre)
            mover_a_respaldo(service, file_id, th_id, respaldo_id)
            try:
                os.remove(local_path)
                print("🗑️ Archivo local eliminado")
            except Exception as e:
                print(f"⚠️ No se pudo eliminar localmente: {e}")

if __name__ == '__main__':
    main()
