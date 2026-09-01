import type { ReactNode } from "react";

type IntelligenceHeaderProps = {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
  marker?: string;
};

export function IntelligenceHeader({ eyebrow, title, description, actions, marker = "DECISION DESK" }: IntelligenceHeaderProps): React.ReactElement {
  return (
    <header className="intelligence-header">
      <div className="intelligence-header__marker" aria-hidden="true">
        <span>NXS</span>
        <small>{marker}</small>
      </div>
      <div className="intelligence-header__copy">
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {actions ? <div className="intelligence-header__actions">{actions}</div> : null}
    </header>
  );
}
