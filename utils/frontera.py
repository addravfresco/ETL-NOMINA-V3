import os

ruta_original = r"Y:\AECF_0101_Anexo4.csv"

print("\n[FORENSE] Iniciando radar de Búsqueda Binaria (O(log N))...")
tamaño_total = os.path.getsize(ruta_original)

limite_inferior = 0
limite_superior = tamaño_total
frontera_exacta = -1

# 1. Encontramos el byte exacto donde empieza la corrupción
while limite_inferior <= limite_superior:
    mitad = (limite_inferior + limite_superior) // 2
    
    with open(ruta_original, "rb") as f:
        f.seek(mitad)
        byte = f.read(1)
        
        if not byte:
            break
            
        if byte == b"\x00":
            # El vacío está aquí, busquemos hacia atrás
            frontera_exacta = mitad
            limite_superior = mitad - 1
        else:
            # Hay datos válidos aquí, busquemos hacia adelante
            limite_inferior = mitad + 1

if frontera_exacta != -1:
    print(f"\n[INFO] ¡Frontera del Abismo localizada en el byte exacto: {frontera_exacta:,}!")
    print(f"[INFO] Volumen de datos reales: {frontera_exacta / (1024**3):.2f} GB")
    
    # 2. Leemos el último pedazo de texto justo antes de chocar con los nulos
    with open(ruta_original, "rb") as f:
        # Retrocedemos 1000 bytes desde la frontera para ver el último registro
        inicio_lectura = max(0, frontera_exacta - 1000)
        f.seek(inicio_lectura)
        ultimo_fragmento = f.read(frontera_exacta - inicio_lectura)
        
        print("\n" + "="*70)
        print(" EL ÚLTIMO REGISTRO QUE ESCRIBIÓ EL SAT ANTES DE MORIR")
        print("="*70)
        # Decodificamos lo que haya podido sobrevivir
        print(ultimo_fragmento.decode("cp1252", errors="replace"))
        print("="*70 + "\n")
else:
    print("[INFO] No se encontraron bytes nulos. Archivo íntegro.")