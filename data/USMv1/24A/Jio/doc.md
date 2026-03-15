# EMS NBI Specification Document

# Overview

This document defines the Northbound Interface (NBI) specification for the EMS system.

## Purpose

Defines the interfaces between EMS (Element Management System) and upper-level management systems (NMS).

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

# Performance Management (PM)

## PM SFTP Interface

{{SFTP:doc.yaml:PM}}

# Appendix

## Revision History

| Version | Date | Author | Description |
|---------|------|--------|-------------|
| 1.0  | 2026-01-15 | J. Kim | Initial draft |
| 1.1  | 2026-02-20 | S. Park | REST API update |
| 1.2  | 2026-03-04 | System | resource.yaml consolidation |
| 2.0  | 2026-03-05 | System | CM/FM/PM domain restructure |
