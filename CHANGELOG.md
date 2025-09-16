# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2025-09-16
### Added
- Dept-aware budget vs actual mart.
- AU freshness thresholds (warn 48h / error 96h).
- Schema tests for key models.

### Changed
- App now reads Live budget table with CSV fallback and shows Budget (MTD) and % Used (MTD) KPIs.
