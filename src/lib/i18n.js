import fs from 'fs';
import path from 'path';

/**
 * Loads the localization messages for the given language.
 * Returns a Proxy object that handles method calls dynamically.
 * e.g., m.hero_title() -> returns translated string.
 */
export function loadLocale(lang) {
  const language = lang || 'it';
  const filePath = path.resolve(`./src/messages/${language}.json`);
  
  let content = '{}';
  try {
    content = fs.readFileSync(filePath, 'utf-8');
  } catch (e) {
    try {
      const fallbackPath = path.resolve(`./src/messages/en.json`);
      content = fs.readFileSync(fallbackPath, 'utf-8');
    } catch (err) {
      console.error('Error loading translation file:', err);
    }
  }

  const translations = JSON.parse(content);

  return new Proxy(translations, {
    get(target, prop) {
      if (typeof prop === 'string') {
        const val = target[prop];
        if (val !== undefined) {
          return () => val;
        }
        // Fallback to the property name if the translation is missing
        return () => prop;
      }
      return Reflect.get(target, prop);
    }
  });
}

/**
 * Returns static paths for the available languages.
 */
export function getLanguagePaths() {
  return [
    { params: { lang: 'en' } },
    { params: { lang: 'it' } }
  ];
}
