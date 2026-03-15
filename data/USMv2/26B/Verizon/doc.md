# EMS NBI Specification Document (USMv2) (26B)

# Overview

This document defines the Northbound Interface (NBI) specification for the EMS system, version 26B with enhanced security and v2 API.

## Purpose

Defines the interfaces between EMS (Element Management System) and upper-level management systems (NMS). This release includes API v2 upgrades, enhanced SFTP security, and new monitoring capabilities.

## System Architecture

The following sequence diagram illustrates the communication flow between EMS and NMS.

{{SEQUENCE:doc.yaml:main_sequence}}

# Configuration Management (CM)

## CM REST API

{{REST:doc.yaml:CM}}

## CM SFTP Interface

{{SFTP:doc.yaml:CM}}

# Fault Management (FM)

## FM REST API

{{REST:doc.yaml:FM}}

## FM SFTP Interface

{{SFTP:doc.yaml:FM}}

# Performance Management (PM)

## PM REST API

{{REST:doc.yaml:PM}}

## PM SFTP Interface

{{SFTP:doc.yaml:PM}}

# Appendix

## Revision History

| Version | Date | Author | Description |
|---------|------|--------|-------------|
| 1.0  | 2026-01-15 | J. Kim | Initial draft |
| 1.1  | 2026-02-20 | S. Park | REST API update |
| 1.2  | 2026-03-04 | System | resource.yaml consolidation |
| 3.0  | 2026-03-05 | System | CM/FM/PM domain restructure, API v2 |
