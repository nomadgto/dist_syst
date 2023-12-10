import socket
import threading
import signal
import sys
import sqlite3
import random
import time
from prettytable import PrettyTable

# Clase NODO
class Nodo:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self.semaphore = threading.Semaphore()
        self.is_running = True

    # Función que se ejecutará cuando se reciba una interrupción (Ctrl+C o Ctrl+Z)
    def signal_handler(self, sig, frame):
        print("\n")
        sys.exit(1)

    # Función que se ejecutará cuando se reciba la señal Ctrl+Z
    def signal_stop_handler(self, sig, frame):
        print("\n")
        sys.exit(1)

    # Función para manejar la comunicación con un nodo remoto
    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024).decode()
            if data:
                if data == 'acquire_permission':
                    self.semaphore.acquire()
                    client_socket.send("authorized_permission".encode())
                elif data == 'release_permission':
                    self.semaphore.release()
                else:
                    local_connection = sqlite3.connect(self.db_path)
                    cursor = local_connection.cursor()
                    parts = data.split('|')

                    if parts[0] == 'create_cliente' and len(parts) == 5:
                        usuario, nombre, direccion, tarjeta = parts[1:]
                        self.create_cliente(cursor, usuario, nombre, direccion, int(tarjeta))
                    elif parts[0] == 'update_cliente' and len(parts) == 5:
                        usuario, nombre, direccion, tarjeta = parts[1:]
                        self.update_cliente(cursor, usuario, nombre, direccion, int(tarjeta))
                    elif parts[0] == 'activate_cliente' and len(parts) == 2:
                        usuario = parts[1]
                        self.activate_cliente(cursor, usuario)
                    elif parts[0] == 'deactivate_cliente' and len(parts) == 2:
                        usuario = parts[1]
                        self.deactivate_cliente(cursor, usuario)
                    elif parts[0] == 'create_articulo' and len(parts) == 5:
                        codigo, nombre, precio, id_sucursal = parts[1:]
                        self.create_articulo(cursor, int(codigo), nombre, float(precio), int(id_sucursal))
                    elif parts[0] == 'update_articulo' and len(parts) == 4:
                        codigo, nombre, precio = parts[1:]
                        self.update_articulo(cursor, int(codigo), nombre, float(precio))
                    elif parts[0] == 'restock_articulo' and len(parts) == 2:
                        codigo = parts[1]
                        self.restock_articulo(cursor, int(codigo))
                    elif parts[0] == 'deactivate_articulo' and len(parts) == 2:
                        codigo = parts[1]
                        self.deactivate_articulo(cursor, int(codigo))
                    elif parts[0] == 'create_guia_envio' and len(parts) == 7:
                        id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra = parts[1:]
                        self.create_guia_envio(cursor, int(id_cliente), int(id_articulo), int(id_sucursal), int(serie), float(monto_total), fecha_compra)
                    
                    cursor.close()
                    local_connection.close()
        except Exception as e:
            print(f"\n>> Error al recibir datos del cliente: {e} \n")
        finally:
            client_socket.close()

    # Función para iniciar el servidor en un nodo
    def start_server(self, ip, port):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind((ip, port))
            server.listen(50)

            while self.is_running:  # Verifica la bandera de ejecución
                client, addr = server.accept()
                client_handler = threading.Thread(target=self.handle_client, args=(client,))
                client_handler.start()
        except OSError as e:
            sys.exit(1)
        finally:
            server.close()  # Cierra el socket del servidor

    def create_tables(self):
        self.create_table("CLIENTE", """
            id_cliente INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT NOT NULL UNIQUE,
            nombre TEXT NOT NULL,
            direccion TEXT NOT NULL,
            tarjeta INTEGER NOT NULL UNIQUE,
            status TEXT NOT NULL CHECK (status IN ('Activo', 'Inactivo'))
        """)

        self.create_table("SUCURSAL", """
            id_sucursal INTEGER PRIMARY KEY NOT NULL,
            ip TEXT NOT NULL UNIQUE,
            nodo_actual INTEGER NOT NULL CHECK (nodo_actual IN (0, 1)),
            nodo_maestro INTEGER NOT NULL CHECK (nodo_maestro IN (0, 1)),
            status INTEGER NOT NULL CHECK (status IN (0, 1)),
            capacidad INTEGER NOT NULL,
            espacio_usado INTEGER NOT NULL
        """)

        self.create_table("ARTICULO", """
            id_articulo INTEGER PRIMARY KEY AUTOINCREMENT,
            id_sucursal INTEGER NOT NULL,
            codigo INTEGER NOT NULL UNIQUE,
            nombre TEXT NOT NULL,
            precio REAL NOT NULL,
            stock TEXT NOT NULL CHECK (stock IN ('Disponible', 'Agotado')),
            FOREIGN KEY (id_sucursal) REFERENCES SUCURSAL(id_sucursal)
        """)

        self.create_table("GUIA_ENVIO", """
            id_guia INTEGER PRIMARY KEY AUTOINCREMENT,
            id_cliente INTEGER NOT NULL,
            id_articulo INTEGER NOT NULL,
            id_sucursal INTEGER NOT NULL,
            serie INTEGER NOT NULL UNIQUE,
            monto_total REAL NOT NULL,
            fecha_compra TEXT NOT NULL,
            FOREIGN KEY (id_cliente) REFERENCES CLIENTE(id_cliente),
            FOREIGN KEY (id_articulo) REFERENCES ARTICULO(id_articulo),
            FOREIGN KEY (id_sucursal) REFERENCES SUCURSAL(id_sucursal)
        """)

    def create_table(self, table_name, fields):
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({fields})
        """)
        self.connection.commit()

    def insert_initial_sucursales(self):
        sucursales_data = [
            (1, '192.168.222.130', 1, 0, 1, 2,  0),
            (2, '192.168.222.128', 0, 0, 1, 3,  0),
            (3, '192.168.222.131', 0, 0, 1, 5,  0),
            (4, '192.168.222.132', 0, 0, 1, 7,  0),
            (5, '192.168.222.133', 0, 1, 1, 11, 0)
        ]

        for sucursal_data in sucursales_data:
            self.cursor.execute("""
                INSERT OR IGNORE INTO SUCURSAL (id_sucursal, ip, nodo_actual, nodo_maestro, status, capacidad, espacio_usado)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, sucursal_data)
            self.connection.commit()

    def pretty_table_query(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        table = PrettyTable([description[0] for description in self.cursor.description])
        table.add_rows(rows)
        print(table)

    def create_cliente(self, cursor, usuario, nombre, direccion, tarjeta):
        status = "Activo"
        cursor.execute("""
            INSERT INTO CLIENTE (usuario, nombre, direccion, tarjeta, status)
            VALUES (?, ?, ?, ?, ?)
        """, (usuario, nombre, direccion, tarjeta, status))
        cursor.connection.commit()

    def read_cliente(self):
        self.pretty_table_query("CLIENTE")

    def update_cliente(self, cursor, usuario, nombre, direccion, tarjeta):
        cursor.execute("""
            UPDATE CLIENTE
            SET nombre = ?, direccion = ?, tarjeta = ?
            WHERE usuario = ?
        """, (nombre, direccion, tarjeta, usuario))
        cursor.connection.commit()

    def activate_cliente(self, cursor, usuario):
        cursor.execute("""
            UPDATE CLIENTE
            SET status = 'Activo'
            WHERE usuario = ?
        """, (usuario,))
        cursor.connection.commit()

    def deactivate_cliente(self, cursor, usuario):
        cursor.execute("""
            UPDATE CLIENTE
            SET status = 'Inactivo'
            WHERE usuario = ?
        """, (usuario,))
        cursor.connection.commit()

    def create_articulo(self, cursor, codigo, nombre, precio, id_sucursal):
        stock = "Disponible"
        cursor.execute("""
            INSERT INTO ARTICULO (id_sucursal, codigo, nombre, precio, stock)
            VALUES (?, ?, ?, ?, ?)
        """, (id_sucursal, codigo, nombre, precio, stock))
        cursor.connection.commit()

    def read_articulo(self):
        self.pretty_table_query("ARTICULO")

    def update_articulo(self, cursor, codigo, nombre, precio):
        cursor.execute("""
            UPDATE ARTICULO
            SET nombre = ?, precio = ?
            WHERE codigo = ?
        """, (nombre, precio, codigo))
        cursor.connection.commit()

    def restock_articulo(self, cursor, codigo):
        cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Disponible'
            WHERE codigo = ? AND stock = 'Agotado'
        """, (codigo,))
        cursor.connection.commit()

    def deactivate_articulo(self, cursor, codigo):
        cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Agotado'
            WHERE codigo = ? AND stock = 'Disponible'
        """, (codigo,))
        cursor.connection.commit()

    def create_guia_envio(self, cursor, id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra):
        cursor.execute("""
            INSERT INTO GUIA_ENVIO (id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra))

        cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Agotado'
            WHERE id_articulo = ? AND stock = 'Disponible'
        """, (id_articulo,))
        cursor.connection.commit()

    def check_cliente_activo(self, usuario):
        self.cursor.execute("SELECT status FROM CLIENTE WHERE usuario = ?", (usuario,))
        status = self.cursor.fetchone()

        if status and status[0] == 'Activo':
            return True
        else:
            return False

    def check_articulo_disponible(self, codigo):
        self.cursor.execute("SELECT stock FROM ARTICULO WHERE codigo = ?", (codigo,))
        stock = self.cursor.fetchone()

        if stock and stock[0] == 'Disponible':
            return True
        else:
            return False

    def read_guia_envio(self):
        self.pretty_table_query("GUIA_ENVIO")

    def estado_sucursales(self):
        print("\n=== Estado de Sucursales ===")
        self.pretty_table_query("SUCURSAL")

    def get_cliente_id(self, usuario):
        self.cursor.execute("SELECT id_cliente FROM CLIENTE WHERE usuario = ?", (usuario,))
        return self.cursor.fetchone()[0]

    def get_articulo_id(self, codigo):
        self.cursor.execute("SELECT id_articulo FROM ARTICULO WHERE codigo = ?", (codigo,))
        return self.cursor.fetchone()[0]

    def get_articulo_price(self, codigo):
        self.cursor.execute("SELECT precio FROM ARTICULO WHERE codigo = ?", (codigo,))
        return self.cursor.fetchone()[0]

    def get_current_sucursal_id(self):
        self.cursor.execute("SELECT id_sucursal FROM SUCURSAL WHERE nodo_actual = 1 AND status = 1")
        return self.cursor.fetchone()[0]

    def get_current_sucursal_ip(self):
        self.cursor.execute("SELECT ip FROM SUCURSAL WHERE nodo_actual = 1 AND status = 1")
        return self.cursor.fetchone()[0]
    
    def get_master_node_ip(self):
        self.cursor.execute("SELECT ip FROM SUCURSAL WHERE nodo_maestro = 1 AND status = 1")
        return self.cursor.fetchone()[0]

    def update_sucursal_info(self, cursor, nodo_id, status, espacio_usado):
        cursor.execute("""
            UPDATE SUCURSAL
            SET status = ?, espacio_usado = ?
            WHERE id_sucursal = ?
        """, (status, espacio_usado, nodo_id))
        cursor.connection.commit()

    # Método para verificar si el usuario existe
    def check_user_exists(self, usuario):
        self.cursor.execute("SELECT 1 FROM CLIENTE WHERE usuario = ?", (usuario,))
        return bool(self.cursor.fetchone())

    # Método para verificar si el código existe
    def check_code_exists(self, codigo):
        self.cursor.execute("SELECT 1 FROM ARTICULO WHERE codigo = ?", (codigo,))
        return bool(self.cursor.fetchone())

    # Función para enviar mensajes a un nodo específico
    def send_message_to_node(self, ip, message):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, 2222))
        client_socket.send(f"{message}".encode())
        client_socket.close()

    # Función para enviar mensajes a todos los nodos actuales
    def send_messages_to_nodes(self, message):
        self.cursor.execute("SELECT ip FROM SUCURSAL WHERE nodo_actual = 0 AND status = 1")
        nodes_ips = self.cursor.fetchall()
        for ip in nodes_ips:
            self.send_message_to_node(ip[0], message)

    def acquire_permission(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            master_ip = self.get_master_node_ip()
            client_socket.connect((master_ip, 2222))
            client_socket.send("acquire_permission".encode())

            data = client_socket.recv(1024).decode()
            if data == "authorized_permission":
                print("\n>> Permiso autorizado.")
            client_socket.close()
        except ConnectionRefusedError:
            client_socket.close()
            print(f"\n>> Error: Se cayo el nodo maestro {master_ip}")
        except OSError as e:
            if "[Errno 113] No route to host" in str(e):
                client_socket.close()
                print("\n>> Error: No hay ruta al host.")

    def release_permission(self):
        master_ip = self.get_master_node_ip()
        self.send_message_to_node(master_ip, "release_permission")

    def main_menu(self):
        while True:
            print("\n=== Menú Principal ===")
            print("1. Operaciones con Clientes")
            print("2. Operaciones con Artículos")
            print("3. Operaciones con Guías de Envío")
            print("4. Estado de Sucursales")
            print("0. Salir")

            choice = input(">> Ingrese su opción: ")
            if choice == '1':
                self.cliente_menu()
            elif choice == '2':
                self.articulo_menu()
            elif choice == '3':
                self.guia_envio_menu()
            elif choice == '4':
                self.estado_sucursales()
            elif choice == '0':
                self.is_running = False
                break
            else:
                print("\n>> Opción no válida. Intente de nuevo.")
        print("\n>> Ctrl+Z o Ctrl+C para finalizar el programa.")

    def cliente_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Clientes ===")
            print("1. Crear Cliente")
            print("2. Leer Clientes")
            print("3. Actualizar Cliente")
            print("4. Activar Cliente")
            print("5. Desactivar Cliente")
            print("0. Volver al Menú Principal")

            choice = input(">> Ingrese su opción: ")
            if choice == '1':
                usuario = input(">> Ingrese el usuario: ")
    
                # Verificar si el usuario ya existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if not user_exists:
                    nombre = input(">> Ingrese el nombre: ")
                    direccion = input(">> Ingrese la dirección: ")
                    tarjeta = int(input(">> Ingrese el número de tarjeta: "))

                    self.acquire_permission()

                    message = f"create_cliente|{usuario}|{nombre}|{direccion}|{tarjeta}"
                    self.send_messages_to_nodes(message)

                    self.create_cliente(self.cursor, usuario, nombre, direccion, tarjeta)

                    self.release_permission()
            elif choice == '2':
                self.read_cliente()
            elif choice == '3':
                usuario = input(">> Ingrese el usuario del cliente a actualizar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    nombre = input(">> Ingrese el nuevo nombre: ")
                    direccion = input(">> Ingrese la nueva dirección: ")
                    tarjeta = int(input(">> Ingrese la nueva tarjeta: "))

                    self.acquire_permission()

                    message = f"update_cliente|{usuario}|{nombre}|{direccion}|{tarjeta}"
                    self.send_messages_to_nodes(message)
                    
                    self.update_cliente(self.cursor, usuario, nombre, direccion, tarjeta)

                    self.release_permission()
            elif choice == '4':
                usuario = input(">> Ingrese el usuario del cliente a activar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    self.acquire_permission()

                    message = f"activate_cliente|{usuario}"
                    self.send_messages_to_nodes(message)

                    self.activate_cliente(self.cursor, usuario)

                    self.release_permission()
            elif choice == '5':
                usuario = input(">> Ingrese el usuario del cliente a desactivar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    self.acquire_permission()

                    message = f"deactivate_cliente|{usuario}"
                    self.send_messages_to_nodes(message)
                    
                    self.deactivate_cliente(self.cursor, usuario)

                    self.release_permission()
            elif choice == '0':
                break
            else:
                print("\n>> Opción no válida. Intente de nuevo.")

    def articulo_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Artículos ===")
            print("1. Crear Artículo")
            print("2. Leer Artículos")
            print("3. Actualizar Artículo")
            print("4. Re-stock Artículo")
            print("5. Desactivar Artículo")
            print("0. Volver al Menú Principal")

            choice = input(">> Ingrese su opción: ")
            if choice == '1':
                codigo = int(input(">> Ingrese el código del artículo: "))
    
                # Verificar si el código ya existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if not code_exists:
                    nombre = input(">> Ingrese el nombre del artículo: ")
                    precio = float(input(">> Ingrese el precio del artículo: "))
                    id_sucursal = self.get_current_sucursal_id()

                    self.acquire_permission()
                    
                    message = f"create_articulo|{codigo}|{nombre}|{precio}|{id_sucursal}"
                    self.send_messages_to_nodes(message)
                    
                    self.create_articulo(self.cursor, codigo, nombre, precio, id_sucursal)

                    self.release_permission()
            elif choice == '2':
                self.read_articulo()
            elif choice == '3':
                codigo = int(input(">> Ingrese el código del artículo a actualizar: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    nombre = input(">> Ingrese el nuevo nombre: ")
                    precio = float(input(">> Ingrese el nuevo precio: "))

                    self.acquire_permission()

                    message = f"update_articulo|{codigo}|{nombre}|{precio}"
                    self.send_messages_to_nodes(message)

                    self.update_articulo(self.cursor, codigo, nombre, precio)

                    self.release_permission()
            elif choice == '4':
                codigo = int(input(">> Ingrese el código del artículo a reabastecer: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    self.acquire_permission()

                    message = f"restock_articulo|{codigo}"
                    self.send_messages_to_nodes(message)

                    self.restock_articulo(self.cursor, codigo)

                    self.release_permission()
            elif choice == '5':
                codigo = int(input(">> Ingrese el código del artículo a desactivar: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    self.acquire_permission()

                    message = f"deactivate_articulo|{codigo}"
                    self.send_messages_to_nodes(message)
                    
                    self.deactivate_articulo(self.cursor, codigo)

                    self.release_permission()
            elif choice == '0':
                break
            else:
                print("\n>> Opción no válida. Intente de nuevo.")

    def guia_envio_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Guías de Envío ===")
            print("1. Comprar")
            print("2. Leer Guías de Envío")
            print("0. Volver al Menú Principal")

            choice = input(">> Ingrese su opción: ")
            if choice == '1':
                usuario = input(">> Ingrese el usuario del cliente: ")
                codigo = int(input(">> Ingrese el código del artículo: "))

                # Verificar si el usuario y código existen y tienen los formatos correctos
                user_exists = self.check_user_exists(usuario)
                code_exists = self.check_code_exists(codigo)

                if user_exists and code_exists:
                    # Verificar si el usuario está activo y si hay stock
                    usuario_activo = self.check_cliente_activo(usuario)
                    stock_disponible = self.check_articulo_disponible(codigo)

                    if usuario_activo and stock_disponible:
                        # Obtener los datos necesarios
                        id_cliente = self.get_cliente_id(usuario)
                        id_articulo = self.get_articulo_id(codigo)
                        id_sucursal = self.get_current_sucursal_id()
                        serie = int(time.strftime("%Y")) + int(time.strftime("%m")) + int(time.strftime("%d")) + int(time.strftime("%H")) + int(time.strftime("%M")) + int(time.strftime("%S")) + id_sucursal + int(random.randint(1, 100))
                        monto_total = self.get_articulo_price(codigo)
                        fecha_compra = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                        self.acquire_permission()

                        message = f"create_guia_envio|{id_cliente}|{id_articulo}|{id_sucursal}|{serie}|{monto_total}|{fecha_compra}"
                        self.send_messages_to_nodes(message)

                        self.create_guia_envio(self.cursor, id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra)

                        self.release_permission()
            elif choice == '2':
                self.read_guia_envio()
            elif choice == '0':
                break
            else:
                print("\n>> Opción no válida. Intente de nuevo.")


if __name__ == "__main__":
    nodo = Nodo("nodo.db")
    nodo.create_tables()
    nodo.insert_initial_sucursales()

    # Registra la función de manejo de señales para la interrupción (Ctrl+C)
    signal.signal(signal.SIGINT, nodo.signal_handler)

    # Registra la función de manejo de señales para Ctrl+Z (suspender)
    signal.signal(signal.SIGTSTP, nodo.signal_stop_handler)

    # Iniciar el servidor en el nodo
    server_thread = threading.Thread(target=nodo.start_server, args=(nodo.get_current_sucursal_ip(), 2222))
    server_thread.start()
    
    nodo.main_menu()

    server_thread.join()