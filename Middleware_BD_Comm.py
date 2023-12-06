import socket
import threading
import signal
import sys
from sqlitepool import ConnectionPool
import random
import time
from prettytable import PrettyTable

# Clase NODO
class Nodo:
    def __init__(self, db_path):
        self.db_path = db_path
        self.pool = ConnectionPool.load(db_path)
        self.connection = self.pool.connect()
        self.cursor = self.connection.cursor()

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
                #print(f"Mensaje recibido: {data}")
                parts = data.split('|')
                if parts[0] == 'create_cliente' and len(parts) == 5:
                    usuario, nombre, direccion, tarjeta = parts[1:]
                    self.create_cliente(self.cursor, usuario, nombre, direccion, int(tarjeta))
        except Exception as e:
            print(f"Error al recibir datos del cliente: {e}")
        finally:
            client_socket.close()

    # Función para iniciar el servidor en un nodo
    def start_server(self, ip, port):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.bind((ip, port))
            server.listen(20)

            while True:
                client, addr = server.accept()
                client_handler = threading.Thread(target=self.handle_client, args=(client,))
                client_handler.start()
        except OSError as e:
            sys.exit(1)

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
            espacio_usado INTEGER NOT NULL,
            espacio_disponible INTEGER NOT NULL
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
            (1, '192.168.222.130', 1, 0, 1, 5, 0, 5),
            (2, '192.168.222.128', 0, 0, 1, 5, 0, 5),
            (3, '192.168.222.131', 0, 0, 1, 10, 0, 10),
            (4, '192.168.222.132', 0, 0, 1, 10, 0, 10),
            (5, '192.168.222.133', 0, 1, 1, 15, 0, 15)
        ]

        for sucursal_data in sucursales_data:
            self.cursor.execute("""
                INSERT OR IGNORE INTO SUCURSAL (id_sucursal, ip, nodo_actual, nodo_maestro, status, capacidad, espacio_usado, espacio_disponible)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

    def update_cliente(self, usuario, nombre, direccion, tarjeta):
        self.cursor.execute("""
            UPDATE CLIENTE
            SET nombre = ?, direccion = ?, tarjeta = ?
            WHERE usuario = ?
        """, (nombre, direccion, tarjeta, usuario))
        self.connection.commit()

    def activate_cliente(self, usuario):
        self.cursor.execute("""
            UPDATE CLIENTE
            SET status = 'Activo'
            WHERE usuario = ?
        """, (usuario,))
        self.connection.commit()

    def deactivate_cliente(self, usuario):
        self.cursor.execute("""
            UPDATE CLIENTE
            SET status = 'Inactivo'
            WHERE usuario = ?
        """, (usuario,))
        self.connection.commit()

    def create_articulo(self, codigo, nombre, precio, id_sucursal):
        stock = "Disponible"
        self.cursor.execute("""
            INSERT INTO ARTICULO (id_sucursal, codigo, nombre, precio, stock)
            VALUES (?, ?, ?, ?, ?)
        """, (id_sucursal, codigo, nombre, precio, stock))
        self.connection.commit()

    def read_articulo(self):
        self.pretty_table_query("ARTICULO")

    def update_articulo(self, codigo, nombre, precio):
        self.cursor.execute("""
            UPDATE ARTICULO
            SET nombre = ?, precio = ?
            WHERE codigo = ?
        """, (nombre, precio, codigo))
        self.connection.commit()

    def restock_articulo(self, codigo):
        self.cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Disponible'
            WHERE codigo = ? AND stock = 'Agotado'
        """, (codigo,))
        self.connection.commit()

    def deactivate_articulo(self, codigo):
        self.cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Agotado'
            WHERE codigo = ? AND stock = 'Disponible'
        """, (codigo,))
        self.connection.commit()

    def create_guia_envio(self, id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra):
        self.cursor.execute("""
            INSERT INTO GUIA_ENVIO (id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra))

        self.cursor.execute("""
            UPDATE ARTICULO
            SET stock = 'Agotado'
            WHERE id_articulo = ? AND stock = 'Disponible'
        """, (id_articulo,))
        self.connection.commit()

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
        self.cursor.execute("SELECT id_sucursal FROM SUCURSAL WHERE nodo_actual = 1")
        return self.cursor.fetchone()[0]

    def get_current_sucursal_ip(self):
        self.cursor.execute("SELECT ip FROM SUCURSAL WHERE nodo_actual = 1")
        return self.cursor.fetchone()[0]

    def update_sucursal_info(self, nodo_id, status, espacio_usado, espacio_disponible):
        self.cursor.execute("""
            UPDATE SUCURSAL
            SET status = ?, espacio_usado = ?, espacio_disponible = ?
            WHERE id_sucursal = ?
        """, (status, espacio_usado, espacio_disponible, nodo_id))
        self.connection.commit()

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
        self.cursor.execute("SELECT ip FROM SUCURSAL WHERE nodo_actual = 0")
        nodes_ips = self.cursor.fetchall()
        for ip in nodes_ips:
            self.send_message_to_node(ip[0], message)

    def main_menu(self):
        while True:
            print("\n=== Menú Principal ===")
            print("1. Operaciones con Clientes")
            print("2. Operaciones con Artículos")
            print("3. Operaciones con Guías de Envío")
            print("4. Estado de Sucursales")
            print("0. Salir")

            choice = input("Ingrese su opción: ")
            if choice == '1':
                self.cliente_menu()
            elif choice == '2':
                self.articulo_menu()
            elif choice == '3':
                self.guia_envio_menu()
            elif choice == '4':
                self.estado_sucursales()
            elif choice == '0':
                break
            else:
                print("Opción no válida. Intente de nuevo.")

    def cliente_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Clientes ===")
            print("1. Crear Cliente")
            print("2. Leer Clientes")
            print("3. Actualizar Cliente")
            print("4. Activar Cliente")
            print("5. Desactivar Cliente")
            print("0. Volver al Menú Principal")

            choice = input("Ingrese su opción: ")
            if choice == '1':
                usuario = input("Ingrese el usuario: ")
    
                # Verificar si el usuario ya existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if not user_exists:
                    nombre = input("Ingrese el nombre: ")
                    direccion = input("Ingrese la dirección: ")
                    tarjeta = int(input("Ingrese el número de tarjeta: "))

                    message = f"create_cliente|{usuario}|{nombre}|{direccion}|{tarjeta}"
                    self.send_messages_to_nodes(message)

                    self.create_cliente(self.cursor, usuario, nombre, direccion, tarjeta)
            elif choice == '2':
                self.read_cliente()
            elif choice == '3':
                usuario = input("Ingrese el usuario del cliente a actualizar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    nombre = input("Ingrese el nuevo nombre: ")
                    direccion = input("Ingrese la nueva dirección: ")
                    tarjeta = int(input("Ingrese la nueva tarjeta: "))
                    self.update_cliente(usuario, nombre, direccion, tarjeta)
            elif choice == '4':
                usuario = input("Ingrese el usuario del cliente a activar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    self.activate_cliente(usuario)
            elif choice == '5':
                usuario = input("Ingrese el usuario del cliente a desactivar: ")
    
                # Verificar si el usuario existe y tiene el formato correcto
                user_exists = self.check_user_exists(usuario)
    
                if user_exists:
                    self.deactivate_cliente(usuario)
            elif choice == '0':
                break
            else:
                print("Opción no válida. Intente de nuevo.")

    def articulo_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Artículos ===")
            print("1. Crear Artículo")
            print("2. Leer Artículos")
            print("3. Actualizar Artículo")
            print("4. Re-stock Artículo")
            print("5. Desactivar Artículo")
            print("0. Volver al Menú Principal")

            choice = input("Ingrese su opción: ")
            if choice == '1':
                codigo = int(input("Ingrese el código del artículo: "))
    
                # Verificar si el código ya existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if not code_exists:
                    nombre = input("Ingrese el nombre del artículo: ")
                    precio = float(input("Ingrese el precio del artículo: "))
                    id_sucursal = self.get_current_sucursal_id()
                    self.create_articulo(codigo, nombre, precio, id_sucursal)
            elif choice == '2':
                self.read_articulo()
            elif choice == '3':
                codigo = int(input("Ingrese el código del artículo a actualizar: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    nombre = input("Ingrese el nuevo nombre: ")
                    precio = float(input("Ingrese el nuevo precio: "))
                    self.update_articulo(codigo, nombre, precio)
            elif choice == '4':
                codigo = int(input("Ingrese el código del artículo a reabastecer: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    self.restock_articulo(codigo)
            elif choice == '5':
                codigo = int(input("Ingrese el código del artículo a desactivar: "))
    
                # Verificar si el código existe y tiene el formato correcto
                code_exists = self.check_code_exists(codigo)
    
                if code_exists:
                    self.deactivate_articulo(codigo)
            elif choice == '0':
                break
            else:
                print("Opción no válida. Intente de nuevo.")

    def guia_envio_menu(self):
        while True:
            print("\n=== Menú de Operaciones con Guías de Envío ===")
            print("1. Comprar")
            print("2. Leer Guías de Envío")
            print("0. Volver al Menú Principal")

            choice = input("Ingrese su opción: ")
            if choice == '1':
                usuario = input("Ingrese el usuario del cliente: ")
                codigo = int(input("Ingrese el código del artículo: "))

                # Verificar si el usuario y código existen y tienen los formatos correctos
                user_exists = self.check_user_exists(usuario)
                code_exists = self.check_code_exists(codigo)

                if user_exists and code_exists:
                    # Verificar si el usuario está activo
                    usuario_activo = self.check_cliente_activo(usuario)

                    if usuario_activo:
                        # Verificar si hay stock
                        stock_disponible = self.check_articulo_disponible(codigo)

                        if stock_disponible:
                            # Obtener los datos necesarios
                            id_cliente = self.get_cliente_id(usuario)
                            id_articulo = self.get_articulo_id(codigo)
                            id_sucursal = self.get_current_sucursal_id()
                            serie = int(time.strftime("%Y")) + int(time.strftime("%m")) + int(time.strftime("%d")) + int(time.strftime("%H")) + int(time.strftime("%M")) + int(time.strftime("%S")) + id_sucursal + int(random.randint(1, 100))
                            monto_total = self.get_articulo_price(codigo)
                            fecha_compra = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                            # Llamar a la función
                            self.create_guia_envio(id_cliente, id_articulo, id_sucursal, serie, monto_total, fecha_compra)
            elif choice == '2':
                self.read_guia_envio()
            elif choice == '0':
                break
            else:
                print("Opción no válida. Intente de nuevo.")

    def estado_sucursales(self):
        print("\n=== Estado de Sucursales ===")
        self.pretty_table_query("SUCURSAL")


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