import socket
import threading
import time
import signal
import sys

# Función que se ejecutará cuando se reciba una interrupción (Ctrl+C o Ctrl+Z)
def signal_handler(sig, frame):
    print("\n")
    sys.exit(1)

# Función que se ejecutará cuando se reciba la señal Ctrl+Z
def signal_stop_handler(sig, frame):
    print("\n")
    sys.exit(1)

# Función para manejar la comunicación con un nodo remoto
def handle_client(client_socket, node_name, messages_file):
    while True:
        data = client_socket.recv(1024).decode()
        if not data:
            break

        # Obtener la hora del reloj del nodo
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        # Mostrar el mensaje recibido
        print(f"Mensaje recibido en {node_name} de {client_socket.getpeername()}: {data}")

        # Enviar una confirmación de recepción al remitente
        client_socket.send(f"Mensaje recibido en {node_name} a las {current_time}".encode())

        # Almacenar el mensaje recibido en el archivo
        with open(messages_file, 'a') as file:
            file.write(f"<< Message Received from {client_socket.getpeername()} - ({current_time}) {node_name}: {data}\n\n")

    client_socket.close()

# Función para iniciar el servidor en un nodo
def start_server(ip, port, node_name, messages_file):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((ip, port))
        server.listen(20)

        #print(f"Servidor en {node_name} escuchando en {ip}:{port}")
        print(f"Servidor en {node_name} escuchando en {server.getsockname()}")

        while True:
            client, addr = server.accept()
            print(f"\nConexión entrante desde {addr}")
            client_handler = threading.Thread(target=handle_client, args=(client, node_name, messages_file))
            client_handler.start()
    except OSError as e:
        print(f"Error al iniciar el servidor en ({node_name}). {e}")
        sys.exit(1)

def main():

    # Registra la función de manejo de señales para la interrupción (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)

    # Registra la función de manejo de señales para Ctrl+Z (suspender)
    signal.signal(signal.SIGTSTP, signal_stop_handler)

    # Pedir al usuario que ingrese su nodo e IP
    node_name = input(">> Ingresa el nombre de tu nodo: ")
    node_ip = input(">> Ingresa la dirección IP de tu nodo: ")
    node_port = 2222

    # Crear un archivo para almacenar los mensajes
    messages_file = f"{node_name}_messages.txt"

    # Iniciar el servidor en el nodo
    server_thread = threading.Thread(target=start_server, args=(node_ip, node_port, node_name, messages_file))
    server_thread.start()
    time.sleep(0.1)

    # Bucle para enviar mensajes a otros nodos
    while True:
        dest_ip = input(">> Ingresa la dirección IP del nodo destinatario: ")
        dest_port = 2222
        message = input(">> Escribe tu mensaje: ")

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((dest_ip, dest_port))
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            client_socket.send(f"({current_time}) {node_name}: {message}".encode())

            # Almacenar el mensaje a enviar en el archivo
            with open(messages_file, 'a') as file:
                file.write(f">> Message Sent to {client_socket.getpeername()} - ({current_time}) {node_name}: {message}\n\n")

            response = client_socket.recv(1024).decode()
            print(f"Confirmación de recepción: {response}")

            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # Almacenar el mensaje de confirmacion en el archivo
            with open(messages_file, 'a') as file:
                file.write(f"<< Message Received from {client_socket.getpeername()} - ({current_time}) {node_name}: Confirmación de recepción: {response}\n\n")

        except socket.gaierror as e:
            print(f"Error de resolución de dirección IP. {e}")

        except ConnectionRefusedError as e:
            print(f"El nodo ({dest_ip}) rechazó la conexión. {e}")

        except TimeoutError as e:
            print(f"Se agotó el tiempo de espera al conectar con el nodo ({dest_ip}). {e}")

        except (socket.error, ConnectionError) as e:
            print(f"Error de conexión con el nodo ({dest_ip}). {e}")

        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
