"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: Challenge 2
Stage: 1 - Data parsing
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 01/2024

@deprecated - On utilise challenge2.ipynb à la place. Utilisation de csv au lien de xml.
"""
import xml.sax
import ipaddress
import pandas as pd


class FlowHandler(xml.sax.ContentHandler):
    def __init__(self, file_size):
        self.current_data = ""
        self.flows = []
        self.flow = {}
        self.file_size = file_size
        self.current_size = 0
        self.last_reported_percent = -1  # Suivre le dernier pourcentage affiché

    def startElement(self, tag, attributes):
        self.current_data = tag
        if tag == "FFlow":
            self.flow = {}

    def endElement(self, tag):
        if tag == "FFlow":
            # Preprocess the data
            self.flow.pop("FFlow")
            self.flow.pop("Timestamp")
            self.flow["src_ip_PrivateNetwork"] = 0
            self.flow["src_ip_PublicNetwork"] = 0
            self.flow["src_ip_MulticastNetwork"] = 0
            self.flow["src_ip_UnknownNetwork"] = 0
            src_ip = self.map_ip_to_interval(self.flow["Src_IP_Add"])
            self.flow[f"src_ip_{src_ip}"] = 1
            self.flow.pop("Src_IP_Add")

            self.flow["dst_ip_PrivateNetwork"] = 0
            self.flow["dst_ip_PublicNetwork"] = 0
            self.flow["dst_ip_MulticastNetwork"] = 0
            self.flow["dst_ip_UnknownNetwork"] = 0
            dst_ip = self.map_ip_to_interval(self.flow["Dst_IP_Add"])
            self.flow[f"dst_ip_{dst_ip}"] = 1
            self.flow.pop("Dst_IP_Add")

            self.flow["src_port_WellKnownPorts"] = 0
            self.flow["src_port_RegisteredPorts"] = 0
            self.flow["src_port_DynamicPrivatePorts"] = 0
            src_port = self.map_port_to_interval(
                self.convert_to_number(self.flow["Src_Pt"])
            )
            self.flow[f"src_port_{src_port}"] = 1
            self.flow.pop("Src_Pt")

            self.flow["dst_port_WellKnownPorts"] = 0
            self.flow["dst_port_RegisteredPorts"] = 0
            self.flow["dst_port_DynamicPrivatePorts"] = 0
            dst_port = self.map_port_to_interval(
                self.convert_to_number(self.flow["Dst_Pt"])
            )
            self.flow[f"dst_port_{dst_port}"] = 1
            self.flow.pop("Dst_Pt")

            self.flow["protocol_TCP"] = 0
            self.flow["protocol_UDP"] = 0
            self.flow["protocol_ICMP"] = 0
            self.flow["protocol_IGMP"] = 0
            self.flow["protocol_Other"] = 0
            protocol = self.flow["Protocol"]
            self.flow[f"protocol_{protocol}"] = 1
            self.flow.pop("Protocol")

            try:
                duration_float = float(self.flow["Duration"])
                self.flow["duration"] = int(duration_float)
            except ValueError:
                self.flow["duration"] = 0
            self.flow.pop("Duration")

            self.flow["packets_Low"] = 0
            self.flow["packets_Medium"] = 0
            self.flow["packets_High"] = 0
            packets_number = self.convert_to_number(self.flow["Packets"])
            packets = self.map_packets_to_interval(packets_number)
            self.flow[f"packets_{packets}"] = 1
            self.flow.pop("Packets")

            self.flow["bytes_Small"] = 0
            self.flow["bytes_Medium"] = 0
            self.flow["bytes_Large"] = 0
            bytes_number = self.convert_to_number(self.flow["Bytes"])
            bytes = self.map_bytes_to_interval(bytes_number)
            self.flow.pop("Bytes")

            self.flow["flows"] = int(self.flow["Flows"])
            self.flow.pop("Flows")

            self.flow["flags_A"] = 0
            self.flow["flags_F"] = 0
            self.flow["flags_S"] = 0
            self.flow["flags_R"] = 0
            self.flow["flags_P"] = 0
            flags = self.parse_flags_to_list(self.flow["Flags"])
            self.flow.update(flags)
            self.flow.pop("Flags")

            self.flow["tos"] = int(self.flow["Tos"])
            self.flow.pop("Tos")

            self.flow["tag_normal"] = 0
            self.flow["tag_attack"] = 0
            self.flow["tag_victim"] = 0
            if self.flow["Tag"] == "normal":
                self.flow["tag_normal"] = 1
            elif self.flow["Tag"] == "attack":
                self.flow["tag_attack"] = 1
            elif self.flow["Tag"] == "victim":
                self.flow["tag_victim"] = 1
            self.flow.pop("Tag")

            self.flows.append(self.flow)
            self.flow = {}  # Réinitialiser le dictionnaire
            # print(f"Flow: {self.flow}")
            # breakpoint()

    def map_ip_to_interval(self, ip):
        try:
            ip = ipaddress.IPv4Address(ip)
        except ValueError:
            return "UnknownNetwork"
        if ip <= ipaddress.IPv4Address("128.0.0.0"):
            return "PrivateNetwork"
        elif ip <= ipaddress.IPv4Address("192.0.0.0"):
            return "PublicNetwork"
        elif ip <= ipaddress.IPv4Address("224.0.0.0"):
            return "MulticastNetwork"
        else:
            return "UnknownNetwork"

    def map_port_to_interval(self, port):
        port_bins = [0, 1023, 49151, 65535]
        port_labels = ["WellKnownPorts", "RegisteredPorts", "DynamicPrivatePorts"]
        return pd.cut([port], bins=port_bins, labels=port_labels, include_lowest=True)[
            0
        ]

    def map_packets_to_interval(self, packets):
        packets_bins = [
            0,
            100,
            500,
            float("inf"),
        ]
        packets_labels = ["Low", "Medium", "High"]
        return pd.cut(
            [packets], bins=packets_bins, labels=packets_labels, include_lowest=True
        )[0]

    def map_bytes_to_interval(self, bytes):
        bytes_bins = [
            0,
            10000,
            50000,
            float("inf"),
        ]
        bytes_labels = ["Small", "Medium", "Large"]
        return pd.cut(
            [bytes], bins=bytes_bins, labels=bytes_labels, include_lowest=True
        )[0]

    def parse_flags_to_list(self, flags_list):
        # split the flags string into a list of flags
        flags_list = [flag.strip() for flag in flags_list]
        flags_dict = {}
        for flag in flags_list:
            if flag != ".":
                flags_dict[f"flags_{flag}"] = 1
        return flags_dict

    def convert_to_number(self, value):
        try:
            return int(value)
        except ValueError:
            # Si contient un M, multiplier par 1 000 000
            if value[-1] == "M":
                return int(float(value[:-1]) * 1000000)
            # Si contient un K, multiplier par 1 000
            elif value[-1] == "K":
                return int(float(value[:-1]) * 1000)
            else:
                # print(f"Unknown value: {value}")
                return 0

    def debug(self):
        print(f"Current data: {self.current_data}")
        print(f"Current flow: {self.flow}")
        breakpoint()

    def characters(self, content):
        self.current_size += len(content.encode("utf-8"))
        progress = (self.current_size / self.file_size) * 100
        if (
            self.current_size % 10000 == 0
        ):  # Mettez à jour la progression toutes les 10000 octets lues
            print(f"Progression: {progress:.2f}%")

        # Afficher la progression uniquement si elle a augmenté d'au moins 1%
        # if progress_percent > self.last_reported_percent:
        #     print(f"Progression: {progress_percent}%")
        #     self.last_reported_percent = progress_percent

        # print(f"Current data: {self.current_data}")
        # print(f"Content: {content}")

        # Vérifier si la clé existe déjà dans self.flow, sinon initialiser avec une chaîne vide
        if self.current_data not in self.flow:
            self.flow[self.current_data] = ""

        # Logique pour traiter le contenu
        if (
            self.current_data
            in [
                "Timestamp",
                "Duration",
                "Protocol",
                "Src_IP_Add",
                "Src_Pt",
                "Dst_IP_Add",
                "Dst_Pt",
                "Packets",
                "Bytes",
                "Flows",
                "Flags",
                "Tos",
                "Tag",
            ]
            and self.flow[self.current_data] == ""
        ):
            content = content.strip()
            self.flow[self.current_data] = content
