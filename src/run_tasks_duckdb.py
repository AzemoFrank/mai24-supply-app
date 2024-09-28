import subprocess

def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.returncode

def main():
    commands = [
        "python /home/fazemo/mai24_cmlops_supply_chain/supply_app/src/clean/scrapper_duckdb.py",
        "python /home/fazemo/mai24_cmlops_supply_chain/supply_app/src/clean/clean_a_duckdb.py",
        "python /home/fazemo/mai24_cmlops_supply_chain/supply_app/src/clean/clean_b_duckdb.py",
        "python /home/fazemo/mai24_cmlops_supply_chain/supply_app/src/utils/train_duckdb.py"
    ]

    for command in commands:
        print(f"Exécution de : {command}")
        return_code = run_command(command)
        if return_code != 0:
            print(f"Erreur lors de l'exécution de {command}")
            print(f"Code de retour : {return_code}")
            break  # Stoppe si une commande échoue
        else:
            print(f"Commande exécutée avec succès : {command}")

if __name__ == "__main__":
    main()