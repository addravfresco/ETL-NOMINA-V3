import unicodedata

# Importamos tus 13,000 reglas originales
from cleaning_rules import REEMPLAZOS_MOJIBAKE


def limpiar_texto_sin_tildes_con_enie(texto: str) -> str:
    """
    Quita acentos (á, é, í, ó, ú) pero respeta la Ñ y ñ.
    """
    if not isinstance(texto, str):
        return texto
    
    # 1. Protegemos las Ñ/ñ reemplazándolas por un comodín temporal
    texto_protegido = texto.replace('Ñ', '<ENIE_MAYUS>').replace('ñ', '<ENIE_MINUS>')
    
    # 2. Desarmamos los acentos restantes y los eliminamos
    texto_normalizado = unicodedata.normalize('NFKD', texto_protegido)
    texto_sin_acentos = "".join([c for c in texto_normalizado if not unicodedata.combining(c)])
    
    # 3. Restauramos las Ñ/ñ originales
    texto_final = texto_sin_acentos.replace('<ENIE_MAYUS>', 'Ñ').replace('<ENIE_MINUS>', 'ñ')
    
    return texto_final

def ejecutar_limpieza():
    print("[INFO] Arrancando el motor. Limpiando 13,000+ reglas (conservando la Ñ)...")
    
    archivo_salida = "cleaning_rules_v2.py"
    contador = 0
    
    with open(archivo_salida, mode='w', encoding='utf-8') as f:
        f.write('"""\nDiccionario maestro de saneamiento (Sin acentos, conservando la Ñ).\n"""\n\n')
        f.write("REEMPLAZOS_MOJIBAKE = {\n")
        
        for patron_sucio, valor_limpio in REEMPLAZOS_MOJIBAKE.items():
            # Limpiamos el valor conservando la Ñ
            nuevo_valor = limpiar_texto_sin_tildes_con_enie(valor_limpio)
            
            # Escapamos las barras invertidas y comillas para no romper Python
            patron_seguro = patron_sucio.replace('\\', '\\\\').replace('"', '\\"')
            valor_seguro = nuevo_valor.replace('\\', '\\\\').replace('"', '\\"')
            
            f.write(f'    r"{patron_seguro}": "{valor_seguro}",\n')
            contador += 1
            
        f.write("}\n")
        
    print(f"[ÉXITO] Se procesaron {contador} reglas.")
    print(f"[ÉXITO] Tu nuevo archivo inmaculado es: '{archivo_salida}'.")

if __name__ == "__main__":
    ejecutar_limpieza()