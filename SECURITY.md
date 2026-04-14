# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Reify, please report it responsibly.

### Contact

**Email:** [yohann.catherine@outlook.fr](mailto:yohann.catherine@outlook.fr)

Please include the following details in your report:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Time

- Acknowledgment: Within 48 hours
- Initial assessment: Within 5 business days
- Fix release: Depends on severity and complexity

## Security Considerations

When using Reify in your applications, please keep the following in mind:

- **SQL Injection**: Reify uses parameterized queries by default. Never concatenate user input directly into SQL.
- **Connection Security**: Always use TLS/SSL for database connections in production.
- **Credential Management**: Store database credentials securely (environment variables, secret managers).
- **Input Validation**: Use the `dto-validation` feature to validate user input before database operations.

## Disclosure Policy

We follow a responsible disclosure process. Once a fix is released, we will:
1. Credit the reporter (if desired)
2. Publish a security advisory
3. Tag the fixed version
