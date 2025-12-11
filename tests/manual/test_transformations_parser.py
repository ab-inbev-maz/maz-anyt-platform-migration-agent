import sys
import os
import shutil
import json
from pathlib import Path
from brewbridge.domain.translators.strategies.transformations_parser.transformations_parser import TransformationsParser

REAL_STATE_PATH = Path("final_state.json") 

def setup_test_environment():
    """Creates temporary folders and templates for the test."""
    base_dir = Path("tests/temp_output")
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True)

    template_path = base_dir / "transformations_template.yaml"
    with open(template_path, "w") as f:
        f.write("# Template generado por engineeringstore\ntransformations: []\n")
    
    return base_dir, template_path

def load_real_artifacts():
    """Loads artifacts from the real JSON file."""
    if not REAL_STATE_PATH.exists():
        print(f"  No se encontró el archivo real en: {REAL_STATE_PATH}")
        print("   Por favor, ajusta la variable REAL_STATE_PATH en el script.")
        return None

    try:
        with open(REAL_STATE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        raw = data.get("raw_artifacts")
        if not raw:
            print(" El JSON no tiene la clave 'raw_artifacts'.")
            return None
            
        return raw
    except Exception as e:
        print(f" Error leyendo JSON: {e}")
        return None

def run_test():
    print(" TEST MANUAL CON DATOS REALES: TransformationsParser")
    print("=====================================================")

    raw_artifacts = load_real_artifacts()
    if not raw_artifacts:
        return

    output_dir, template_path = setup_test_environment()
    
    items = raw_artifacts.get("items", [])
    target_item = None
    
    print(f" Analizando {len(items)} items en el estado...")
    
    for item in items:
        if item.get("notebook_path_slv"):
            target_item = item
            break
    
    if not target_item:
        print(" No se encontró ningún item con 'notebook_path_slv' en el JSON.")
        if len(items) > 0:
            target_item = items[0]
            print(f" Usando el primer item encontrado: {target_item['table_name']}")
        else:
            return

    table_name = target_item["table_name"]
    print(f" Objetivo seleccionado: {table_name}")
    print(f"   Script Silver: {target_item.get('notebook_path_slv')}")

    parser = TransformationsParser(raw_artifacts)
    output_path = output_dir / f"{table_name}_transformations.yaml"

    try:
        print("\n  Ejecutando parser...")
        parser.parse(
            target_table=table_name,
            template_path=str(template_path),
            output_path=str(output_path)
        )
        print(" Parsing finalizado.")
    except Exception as e:
        print(f" Error crítico en parser: {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"\n CONTENIDO GENERADO ({output_path}):")
    print("-" * 50)
    if output_path.exists():
        with open(output_path, "r") as f:
            print(f.read())
    else:
        print(" El archivo de salida no se creó.")
    print("-" * 50)

if __name__ == "__main__":
    run_test()