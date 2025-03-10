import os
import signal

def kill_process(pid):
    """Убивает процесс по его PID"""
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Процесс {pid} успешно завершён.")
    except ProcessLookupError:
        print(f"Процесс {pid} не найден.")
    except PermissionError:
        print(f"Нет прав для завершения процесса {pid}.")

if __name__ == "__main__":
    pid_to_kill = int(input("Введите PID процесса, который нужно убить: "))
    kill_process(pid_to_kill)
