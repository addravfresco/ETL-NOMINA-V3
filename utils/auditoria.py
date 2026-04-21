import os

# Apuntamos al archivo CRUDO original, sin el "utf8_clean"
ruta_original = r"Y:\AECF_0101_Anexo4.csv"

print(f"\n[FORENSE] Analizando el archivo ORIGINAL del SAT: {ruta_original}")

try:
    tamaño_bytes = os.path.getsize(ruta_original)
    print(f"[INFO] Tamaño total físico: {tamaño_bytes / (1024**3):.2f} GB")

    with open(ruta_original, "rb") as f:
        # Movemos el puntero 5000 bytes antes del final
        f.seek(-5000, 2)
        cola_binaria = f.read()

        conteo_enters = cola_binaria.count(b"\n")
        conteo_nulos = cola_binaria.count(b"\x00")

        print("\n" + "="*70)
        print(" DIAGNÓSTICO ESTRUCTURAL (Últimos 5000 bytes)")
        print("="*70)
        print(f"-> Saltos de línea (Enters / \\n): {conteo_enters}")
        print(f"-> Bytes Nulos (Corrupción / \\x00): {conteo_nulos}")
        print("="*70 + "\n")
        
        print(" MUESTRA DE TEXTO FINAL (Decodificación CP1252 / Latin-1)\n")
        # El SAT usa Windows-1252, decodificamos ignorando errores
        texto_final = cola_binaria[-1500:].decode("cp1252", errors="replace")
        print(texto_final)
        
        print("\n" + "="*70)
        
        if conteo_nulos > 4000:
            print("[ALERTA] El archivo ORIGINAL también es pura basura al final. El SAT lo exportó mal.")
        elif conteo_enters > 0:
            print("[ÉXITO] ¡EL ARCHIVO ORIGINAL ESTÁ SANO! El problema fue la conversión a UTF-8.")
            
except Exception as e:
    print(f"Error al analizar: {e}")