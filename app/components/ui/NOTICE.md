# shadcn/ui source attribution

Button, Badge and Sheet are manually adapted from the official `new-york-v4`
registry, retrieved 2026-09-02:

- https://ui.shadcn.com/r/styles/new-york-v4/button.json
- https://ui.shadcn.com/r/styles/new-york-v4/badge.json
- https://ui.shadcn.com/r/styles/new-york-v4/sheet.json

Local adaptation: namespaced CSS uses existing NEXUS tokens instead of enabling
Tailwind Preflight or replacing the application theme. Only used variants are
exposed; Button remains a native button (default type="button"), Badge is static,
and Sheet uses the Radix Dialog package directly. There is no CLI initialization
or automatic upstream source update. Review upstream changes before applying them.
Business status, research admission, and financial calculations remain outside
these components. Input, Tabs and grid dependencies are intentionally deferred.

MIT License

Copyright (c) 2023 shadcn

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
