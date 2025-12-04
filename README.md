# Flows App Template

A template repository for creating new Flows apps with best practices and CI/CD built-in.

## Quick Start

1. **Use this template** - Click "Use this template" button to create a new repository
2. **Run setup** - `npm run setup` to customize placeholders automatically
3. **Implement your logic** - Add blocks and customize configuration
4. **Set up CI/CD** - Configure branch protection and deployment
5. **Release** - Tag and release your app

## Template Structure

```
├── .github/workflows/ci.yml    # CI/CD pipeline
├── .gitignore                  # Git ignore rules
├── package.json                # Dependencies and scripts
├── tsconfig.json              # TypeScript configuration
├── main.ts                    # App definition
├── types.ts                   # Type definitions
├── blocks/                    # Block implementations
│   ├── index.ts              # Block registry and exports
│   └── exampleBlock.ts       # Example block implementation
├── setup.sh                  # Automated setup script
└── README.md                 # This file
```

## Customization Guide

### 1. Replace Placeholders

Find and replace these placeholders throughout the codebase:

- `{{APP_NAME}}` - Your app name (e.g., "Slack Integration")
- `{{APP_DESCRIPTION}}` - Brief description of your app

**Files to update:**

- `package.json` - name and description fields
- `main.ts` - app name and description
- `types.ts` - JSDoc comments
- `README.md` - update this file

### 2. Customize Configuration

In `main.ts`, modify the `config` object:

```typescript
config: {
  type: "object",
  properties: {
    apiKey: {
      type: "string",
      title: "API Key",
      description: "Your service API key",
      secret: true  // This makes it a password field
    },
    baseUrl: {
      type: "string",
      title: "Base URL",
      description: "API base URL",
      default: "https://api.example.com"
    }
  },
  required: ["apiKey"]  // Required fields
}
```

### 3. Implement Your Blocks

The template includes a clean block structure. Blocks are organized in the `blocks/` directory:

#### Adding a New Block

1. **Create the block file** (e.g., `blocks/myNewBlock.ts`):

```typescript
import { AppBlock, EventInput } from "@slflows/sdk";

export const myNewBlock: AppBlock = {
  name: "Your Action Name",
  description: "What your block does",
  category: "Your Category",

  inputs: {
    default: {
      name: "Input Name",
      description: "What users need to provide",
      config: {
        // Define your input schema here
        type: "object",
        properties: {
          message: {
            type: "string",
            title: "Message",
            description: "Input description",
          },
        },
        required: ["message"],
      },
      onEvent: async (input: EventInput, { events }) => {
        // Your logic here
        const message = input.params.message as string;
        const apiKey = input.app.config.apiKey as string;

        // Call your external API, process data, etc.

        // Just emit the result directly - don't wrap in success object
        await events.emit({
          result: "your result",
        });
      },
    },
  },

  outputs: {
    default: {
      name: "Output Name",
      description: "What your block returns",
      default: true,
      type: {
        // Define your output schema here
        type: "object",
        properties: {
          result: { type: "string" },
        },
        required: ["result"],
      } as any,
    },
  },
};
```

2. **Register the block** in `blocks/index.ts`:

```typescript
import { myNewBlock } from "./myNewBlock.ts";

export const blocks = {
  example: exampleBlock,
  myNew: myNewBlock, // Add your block here
} as const;

export { myNewBlock }; // Export for external use
```

That's it! The block will automatically be included in your app via `Object.values(blocks)` in `main.ts`.

## Development

### Prerequisites

- Node.js 20+
- npm

### Setup

```bash
npm install
npm run setup        # Interactive setup to customize template
```

### Available Scripts

```bash
npm run setup        # Customize template placeholders
npm run typecheck    # Type checking
npm run format       # Code formatting
npm run bundle       # Create deployment bundle
```

### Testing Your App

1. Run type checking: `npm run typecheck`
2. Format code: `npm run format`
3. Create bundle: `npm run bundle`

**Note**: Initial `npm run typecheck` will show SDK import errors until you customize the template. This is expected - the SDK will be available when the app runs in the Flows environment.

## CI/CD Pipeline

The template includes a complete CI/CD pipeline in `.github/workflows/ci.yml`:

### Continuous Integration

- **Triggers**: All branch pushes (except main)
- **Steps**: Type check, format validation, bundling
- **Quality Gates**: Must pass all checks to merge

### Automated Releases

- **Triggers**: Semver tags (v1.0.0, v2.1.3, etc.)
- **Process**:
  1. Runs full CI validation
  2. Creates GitHub release with bundle
  3. Updates version registry (`versions.json`)
  4. Pushes registry to main branch

### Version Registry

The pipeline automatically maintains a `versions.json` file:

```json
{
  "versions": [
    {
      "version": "1.0.0",
      "artifactUrl": "https://github.com/user/repo/releases/download/v1.0.0/bundle.tar.gz",
      "artifactChecksum": "sha256:abc123..."
    }
  ]
}
```

## Repository Setup

### 1. Branch Protection (Recommended)

Configure branch protection for `main`:

- Require pull request reviews
- Require status checks (CI)
- Allow GitHub Actions bot to bypass (for version registry updates)

### 2. Repository Settings

- Enable "Template repository" if this will be reused
- Configure secrets if needed for external services
- Set up branch protection rules

## Deployment

### Creating Releases

1. **Ensure main is clean**: All changes merged and CI passing
2. **Create and push tag**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. **Automated process**: CI creates release and updates registry
4. **Verify**: Check GitHub releases and `versions.json` in main

### Versioning

Follow [Semantic Versioning](https://semver.org/):

- `v1.0.0` - Major release (breaking changes)
- `v1.1.0` - Minor release (new features)
- `v1.0.1` - Patch release (bug fixes)

## Best Practices

### Code Organization

- Keep `main.ts` focused on app definition
- Use `types.ts` for all TypeScript definitions
- Document your blocks and configuration clearly

### Error Handling

- Let errors bubble up naturally - don't catch and wrap them
- Use descriptive error messages
- The framework will handle error catching and reporting

### Security

- Mark sensitive config fields as `secret: true`
- Never log API keys or sensitive data
- Validate all inputs

### Testing

- Test your blocks manually before releasing
- Verify configuration schema works as expected
- Test the complete deployment pipeline

## Troubleshooting

### Common Issues

**CI fails on format check**

```bash
npm run format
git add .
git commit -m "Fix formatting"
```

**Bundle creation fails**

- Check TypeScript errors: `npm run typecheck`
- Verify all imports are correct
- Ensure `main.ts` exports default app

**Release creation fails**

- Verify branch protection allows GitHub Actions bot
- Check repository permissions
- Ensure tag follows semver format (v1.0.0)

**Version registry not updating**

- Check GitHub Actions bot has push access to main
- Verify workflow permissions in repository settings
- Check for conflicts in versions.json

## Template Checklist

When creating a new app from this template:

- [ ] Run `npm install && npm run setup` for automated setup
- [ ] Customize app configuration schema in `main.ts`
- [ ] Implement your block logic in `blocks/`
- [ ] Update block names, descriptions, and categories
- [ ] Define proper input/output schemas
- [ ] Test locally with `npm run typecheck` and `npm run bundle`
- [ ] Update this README with app-specific information
- [ ] Set up repository branch protection
- [ ] Create first release with `git tag v1.0.0`

---

**Template Version**: 1.0.0
