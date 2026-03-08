import type React from 'react'
declare module 'react' {
  namespace JSX {
    interface IntrinsicElements {
      'falkordb-canvas': React.DetailedHTMLProps<
        React.HTMLAttributes<HTMLElement>,
        HTMLElement
      >
    }
  }
}
